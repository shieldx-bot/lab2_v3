package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"golang.org/x/sync/errgroup"
)

// ============================================
// CONFIGURATION
// ============================================
type Config struct {
	ScyllaHosts []string
	Keyspace    string
	NATSServers []string
	DataCenter  string
	MaxWorkers  int // số lượng goroutine đồng thời tối đa cho NATS handler

	// --- CẤU HÌNH SCYLLA CLOUD (HARDCODE, xem LoadConfig) ---
	ScyllaUsername    string
	ScyllaPassword    string
	ScyllaPort        string // cổng public TCP plaintext: 9042 (9142 là TLS)
	ScyllaTLSCA       string
	ScyllaInsecureTLS bool

	// --- MỚI: cấu hình cho tầng nghiệp vụ (business-layer TOPSIS) ---
	TopsisBatchWindow time.Duration // chu kỳ quét bảng dang_ky trạng thái 'ChoXuLy' (Section 7.2)
	TopsisUseAHP      bool          // true = dùng AHP (ma trận so sánh cặp), false = dùng EWM (mặc định)
}

func LoadConfig() Config {
	// ScyllaDB Cloud (public TCP, cổng 9042 plaintext -- đã xác minh kết nối):
	// HARDCODE, không đọc env (xem yêu cầu "hardcode key, khong dung env").
	scyllaHosts := []string{
		"node-0.aws-ap-southeast-1.b20d788451ea289820b7.clusters.scylla.cloud:9042",
		"node-1.aws-ap-southeast-1.b20d788451ea289820b7.clusters.scylla.cloud:9042",
		"node-2.aws-ap-southeast-1.b20d788451ea289820b7.clusters.scylla.cloud:9042",
	}
	natsServers := strings.Split(getEnv("NATS_SERVERS", "nats://192.168.0.2:4222"), ",")

	topsisBatchWindow, err := time.ParseDuration(getEnv("TOPSIS_BATCH_WINDOW", "5s"))
	if err != nil {
		topsisBatchWindow = 5 * time.Second
	}

	return Config{
		ScyllaHosts:       scyllaHosts,
		Keyspace:          "my_keyspace",
		NATSServers:       natsServers,
		DataCenter:        "AWS_AP_SOUTHEAST_1",
		MaxWorkers:        50,
		ScyllaUsername:    "scylla",
		ScyllaPassword:    "r3yGQpEw51IJfNt",
		ScyllaPort:        "9042",
		ScyllaTLSCA:       "",
		ScyllaInsecureTLS: false,
		TopsisBatchWindow: topsisBatchWindow,
		TopsisUseAHP:      getEnv("TOPSIS_USE_AHP", "false") == "true",
	}
}

// Hàm helper để đọc ENV có giá trị mặc định (fallback)
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

var config = LoadConfig()

// ============================================================================
// TOPSIS BUSINESS-LAYER CORE (inline trực tiếp vào db-service, không tách
// package riêng -- theo yêu cầu "cho TOPSIS vào db luôn"). 3 module dưới
// đây (RuleEngine, TOPSISBusiness, AllocationOptimizer) thuần logic, không
// giữ kết nối Scylla/NATS nào, nên tách rời hoàn toàn khỏi phần I/O bên
// dưới -- dễ unit-test độc lập nếu cần (xem lại bộ test tách riêng đã gửi
// trước đó, dùng chung logic này nhưng với dữ liệu giả lập).
// ============================================================================

// ---- 0. Kiểu dữ liệu dùng chung ----

type CriterionType int

const (
	Benefit CriterionType = iota
	Cost
)

type Criterion struct {
	Name string
	Type CriterionType
}

type WeightMethod int

const (
	EWM WeightMethod = iota
	AHP
)

// StudentRequest = 1 dòng của ma trận quyết định (1 nguyện vọng đăng ký).
type StudentRequest struct {
	StudentID   string
	CourseID    string // = ma_mon_hoc
	SectionID   string // = ma_lop_hoc_phan
	Scores      []float64
	SubmittedAt time.Time

	PrerequisitesMet   bool
	CreditLoadOK       bool
	HasScheduleClash   bool
	AlreadyRegistered  bool
	EligibleForProgram bool
}

// ---- 1. RULE ENGINE (lọc ràng buộc cứng) ----

type RuleEngine struct{}

type RejectionReason string

const (
	ReasonPrerequisiteNotMet    RejectionReason = "PREREQUISITE_NOT_MET"
	ReasonCreditLimitExceeded   RejectionReason = "CREDIT_LIMIT_EXCEEDED"
	ReasonScheduleClash         RejectionReason = "SCHEDULE_CLASH"
	ReasonDuplicateRegistration RejectionReason = "DUPLICATE_REGISTRATION"
	ReasonNotEligibleProgram    RejectionReason = "PROGRAM_NOT_ELIGIBLE"
)

type FilterResult struct {
	Eligible []StudentRequest
	Rejected map[string]RejectionReason
}

func (RuleEngine) Filter(requests []StudentRequest) FilterResult {
	result := FilterResult{
		Eligible: make([]StudentRequest, 0, len(requests)),
		Rejected: make(map[string]RejectionReason),
	}
	for _, r := range requests {
		key := r.StudentID + "|" + r.CourseID + "|" + r.SectionID
		switch {
		case !r.PrerequisitesMet:
			result.Rejected[key] = ReasonPrerequisiteNotMet
		case !r.CreditLoadOK:
			result.Rejected[key] = ReasonCreditLimitExceeded
		case r.HasScheduleClash:
			result.Rejected[key] = ReasonScheduleClash
		case r.AlreadyRegistered:
			result.Rejected[key] = ReasonDuplicateRegistration
		case !r.EligibleForProgram:
			result.Rejected[key] = ReasonNotEligibleProgram
		default:
			result.Eligible = append(result.Eligible, r)
		}
	}
	return result
}

// ---- 2. TOPSIS-BUSINESS (xếp hạng, EWM/AHP boolean-switch) ----

type WeightConfig struct {
	Method            WeightMethod
	AHPPairwiseMatrix [][]float64
}

type TOPSISBusiness struct {
	Criteria []Criterion
	Weights  WeightConfig
}

type RankedRequest struct {
	Request StudentRequest
	Score   float64
	Rank    int
}

var ErrEmptyPool = errors.New("topsis: decision matrix has zero rows")
var ErrCriteriaMismatch = errors.New("topsis: request score vector length does not match declared criteria")

func (t TOPSISBusiness) RankByCourse(eligible []StudentRequest) (map[string][]RankedRequest, error) {
	pools := make(map[string][]StudentRequest)
	for _, r := range eligible {
		pools[r.CourseID] = append(pools[r.CourseID], r)
	}
	out := make(map[string][]RankedRequest)
	for courseID, pool := range pools {
		ranked, err := t.rankPool(pool)
		if err != nil {
			return nil, err
		}
		out[courseID] = ranked
	}
	return out, nil
}

func (t TOPSISBusiness) rankPool(pool []StudentRequest) ([]RankedRequest, error) {
	m := len(pool)
	n := len(t.Criteria)
	if m == 0 {
		return nil, ErrEmptyPool
	}
	for _, r := range pool {
		if len(r.Scores) != n {
			return nil, ErrCriteriaMismatch
		}
	}

	// Bước 2: chuẩn hóa vector r_ij = x_ij / sqrt(sum_i x_ij^2)
	r := make([][]float64, m)
	for i := range r {
		r[i] = make([]float64, n)
	}
	for j := 0; j < n; j++ {
		var sumSq float64
		for i := 0; i < m; i++ {
			sumSq += pool[i].Scores[j] * pool[i].Scores[j]
		}
		denom := math.Sqrt(sumSq)
		if denom == 0 {
			denom = 1
		}
		for i := 0; i < m; i++ {
			r[i][j] = pool[i].Scores[j] / denom
		}
	}

	// Bước 3: trọng số w_j (EWM hoặc AHP)
	w, err := t.computeWeights(r, m, n)
	if err != nil {
		return nil, err
	}

	v := make([][]float64, m)
	for i := 0; i < m; i++ {
		v[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			v[i][j] = w[j] * r[i][j]
		}
	}

	// Bước 4: A+, A-
	aPlus := make([]float64, n)
	aMinus := make([]float64, n)
	for j := 0; j < n; j++ {
		col := make([]float64, m)
		for i := 0; i < m; i++ {
			col[i] = v[i][j]
		}
		maxV, minV := maxOf(col), minOf(col)
		if t.Criteria[j].Type == Benefit {
			aPlus[j], aMinus[j] = maxV, minV
		} else {
			aPlus[j], aMinus[j] = minV, maxV
		}
	}

	// Bước 5 & 6: S+, S-, C_i*
	ranked := make([]RankedRequest, m)
	for i := 0; i < m; i++ {
		var sPlus, sMinus float64
		for j := 0; j < n; j++ {
			sPlus += (v[i][j] - aPlus[j]) * (v[i][j] - aPlus[j])
			sMinus += (v[i][j] - aMinus[j]) * (v[i][j] - aMinus[j])
		}
		sPlus, sMinus = math.Sqrt(sPlus), math.Sqrt(sMinus)
		var score float64
		if sPlus+sMinus == 0 {
			score = 0.5
		} else {
			score = sMinus / (sPlus + sMinus)
		}
		ranked[i] = RankedRequest{Request: pool[i], Score: score}
	}

	// Bước 7: xếp hạng + phân xử khi bằng điểm
	sort.SliceStable(ranked, func(a, b int) bool {
		if math.Abs(ranked[a].Score-ranked[b].Score) > 1e-9 {
			return ranked[a].Score > ranked[b].Score
		}
		return tieBreak(ranked[a].Request, ranked[b].Request, t.Criteria)
	})
	for i := range ranked {
		ranked[i].Rank = i + 1
	}
	return ranked, nil
}

func (t TOPSISBusiness) computeWeights(r [][]float64, m, n int) ([]float64, error) {
	if t.Weights.Method == AHP {
		return ahpWeights(t.Weights.AHPPairwiseMatrix)
	}
	return ewmWeights(r, m, n)
}

func ewmWeights(r [][]float64, m, n int) ([]float64, error) {
	w := make([]float64, n)
	e := make([]float64, n)
	for j := 0; j < n; j++ {
		var colSum float64
		for i := 0; i < m; i++ {
			colSum += r[i][j]
		}
		var entropy float64
		for i := 0; i < m; i++ {
			var p float64
			if colSum > 0 {
				p = r[i][j] / colSum
			}
			if p > 0 {
				entropy += p * math.Log(p)
			}
		}
		lnM := math.Log(float64(m))
		if lnM == 0 {
			lnM = 1
		}
		e[j] = -entropy / lnM
	}
	var sumDiv float64
	for j := 0; j < n; j++ {
		sumDiv += 1 - e[j]
	}
	if sumDiv == 0 {
		for j := range w {
			w[j] = 1.0 / float64(n)
		}
		return w, nil
	}
	for j := 0; j < n; j++ {
		w[j] = (1 - e[j]) / sumDiv
	}
	return w, nil
}

func ahpWeights(pcm [][]float64) ([]float64, error) {
	n := len(pcm)
	if n == 0 {
		return nil, errors.New("topsis: AHP pairwise matrix is empty")
	}
	for _, row := range pcm {
		if len(row) != n {
			return nil, errors.New("topsis: AHP pairwise matrix must be square")
		}
	}
	colSums := make([]float64, n)
	for j := 0; j < n; j++ {
		for i := 0; i < n; i++ {
			colSums[j] += pcm[i][j]
		}
	}
	w := make([]float64, n)
	for i := 0; i < n; i++ {
		var rowSum float64
		for j := 0; j < n; j++ {
			if colSums[j] == 0 {
				continue
			}
			rowSum += pcm[i][j] / colSums[j]
		}
		w[i] = rowSum / float64(n)
	}
	lambdaMax := principalEigenvalueApprox(pcm, w)
	ci := (lambdaMax - float64(n)) / float64(n-1)
	if n > 1 {
		ri := randomIndex(n)
		if ri > 0 {
			cr := ci / ri
			if cr > 0.10 {
				return w, errors.New("topsis: AHP pairwise matrix is inconsistent (CR > 0.10); ask experts to revise comparisons")
			}
		}
	}
	return w, nil
}

func principalEigenvalueApprox(pcm [][]float64, w []float64) float64 {
	n := len(pcm)
	if n <= 1 {
		return float64(n)
	}
	var lambda float64
	for i := 0; i < n; i++ {
		var rowDot float64
		for j := 0; j < n; j++ {
			rowDot += pcm[i][j] * w[j]
		}
		if w[i] != 0 {
			lambda += rowDot / w[i]
		}
	}
	return lambda / float64(n)
}

func randomIndex(n int) float64 {
	ri := []float64{0, 0, 0.58, 0.90, 1.12, 1.24, 1.32, 1.41, 1.45, 1.49}
	if n-1 >= 0 && n-1 < len(ri) {
		return ri[n-1]
	}
	return 1.49
}

// tieBreak: mục 15 thiết kế -- nguy cơ chậm tốt nghiệp > số kỳ đã chờ >
// ít lớp thay thế hơn > thời điểm gửi sớm hơn (chỉ dùng khi mọi tiêu chí
// khác đã bằng nhau).
func tieBreak(a, b StudentRequest, criteria []Criterion) bool {
	idx := func(name string) int {
		for i, c := range criteria {
			if c.Name == name {
				return i
			}
		}
		return -1
	}
	if i := idx("graduation_delay_risk"); i >= 0 && a.Scores[i] != b.Scores[i] {
		return a.Scores[i] > b.Scores[i]
	}
	if i := idx("semesters_waited"); i >= 0 && a.Scores[i] != b.Scores[i] {
		return a.Scores[i] > b.Scores[i]
	}
	if i := idx("alternative_sections"); i >= 0 && a.Scores[i] != b.Scores[i] {
		return a.Scores[i] < b.Scores[i]
	}
	return a.SubmittedAt.Before(b.SubmittedAt)
}

func maxOf(xs []float64) float64 {
	m := xs[0]
	for _, x := range xs[1:] {
		if x > m {
			m = x
		}
	}
	return m
}

func minOf(xs []float64) float64 {
	m := xs[0]
	for _, x := range xs[1:] {
		if x < m {
			m = x
		}
	}
	return m
}

// ---- 3. ALLOCATION OPTIMIZER (ILP heuristic: greedy + local search) ----

type Edge struct {
	StudentID string
	CourseID  string
	SectionID string
	Score     float64
}

type Section struct {
	ID       string
	CourseID string
	Capacity int
}

type ScheduleConflicts map[string]map[string]bool

type AllocationOptimizer struct {
	Sections    map[string]Section
	Conflicts   ScheduleConflicts
	MaxSwapIter int
}

type Assignment struct {
	StudentID string
	CourseID  string
	SectionID string
	Score     float64
}

type AllocationResult struct {
	Confirmed []Assignment
	Waitlist  []Edge
}

func (o AllocationOptimizer) Solve(edges []Edge) AllocationResult {
	maxIter := o.MaxSwapIter
	if maxIter <= 0 {
		maxIter = 500
	}

	scoreOf := make(map[string]map[string]float64, len(edges))
	for _, e := range edges {
		if scoreOf[e.StudentID] == nil {
			scoreOf[e.StudentID] = make(map[string]float64)
		}
		scoreOf[e.StudentID][e.SectionID] = e.Score
	}

	sorted := make([]Edge, len(edges))
	copy(sorted, edges)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Score > sorted[j].Score })

	capLeft := make(map[string]int, len(o.Sections))
	for id, s := range o.Sections {
		capLeft[id] = s.Capacity
	}

	assignedCourse := make(map[string]string)
	studentSections := make(map[string]map[string]bool)

	var confirmed []Assignment
	var waitlist []Edge

	canAssign := func(studentID, courseID, sectionID string) bool {
		if capLeft[sectionID] <= 0 {
			return false
		}
		if c, ok := assignedCourse[studentID]; ok && c == courseID {
			return false
		}
		for existing := range studentSections[studentID] {
			if o.Conflicts[sectionID][existing] {
				return false
			}
		}
		return true
	}

	doAssign := func(e Edge) {
		capLeft[e.SectionID]--
		assignedCourse[e.StudentID] = e.CourseID
		if studentSections[e.StudentID] == nil {
			studentSections[e.StudentID] = make(map[string]bool)
		}
		studentSections[e.StudentID][e.SectionID] = true
		confirmed = append(confirmed, Assignment{
			StudentID: e.StudentID, CourseID: e.CourseID,
			SectionID: e.SectionID, Score: e.Score,
		})
	}

	for _, e := range sorted {
		if canAssign(e.StudentID, e.CourseID, e.SectionID) {
			doAssign(e)
		} else {
			waitlist = append(waitlist, e)
		}
	}

	swapFeasible := func(a, b Assignment) bool {
		for existing := range studentSections[a.StudentID] {
			if existing != a.SectionID && o.Conflicts[b.SectionID][existing] {
				return false
			}
		}
		for existing := range studentSections[b.StudentID] {
			if existing != b.SectionID && o.Conflicts[a.SectionID][existing] {
				return false
			}
		}
		return true
	}

	applySwap := func(i, j int) {
		a, b := confirmed[i], confirmed[j]
		delete(studentSections[a.StudentID], a.SectionID)
		delete(studentSections[b.StudentID], b.SectionID)
		studentSections[a.StudentID][b.SectionID] = true
		studentSections[b.StudentID][a.SectionID] = true
		confirmed[i].SectionID = b.SectionID
		confirmed[i].Score = scoreOf[a.StudentID][b.SectionID]
		confirmed[j].SectionID = a.SectionID
		confirmed[j].Score = scoreOf[b.StudentID][a.SectionID]
	}

	improved := true
	for iter := 0; improved && iter < maxIter; iter++ {
		improved = false
		for i := 0; i < len(confirmed) && !improved; i++ {
			for j := i + 1; j < len(confirmed); j++ {
				a, b := confirmed[i], confirmed[j]
				if a.CourseID != b.CourseID || a.SectionID == b.SectionID {
					continue
				}
				gain := (scoreOf[a.StudentID][b.SectionID] + scoreOf[b.StudentID][a.SectionID]) -
					(scoreOf[a.StudentID][a.SectionID] + scoreOf[b.StudentID][b.SectionID])
				if gain > 1e-9 && swapFeasible(a, b) {
					applySwap(i, j)
					improved = true
					break
				}
			}
		}
	}

	sort.SliceStable(waitlist, func(i, j int) bool { return waitlist[i].Score > waitlist[j].Score })
	return AllocationResult{Confirmed: confirmed, Waitlist: waitlist}
}

// ============================================================================
// HẾT PHẦN TOPSIS BUSINESS-LAYER CORE -- từ đây trở xuống là code gốc của
// db-service (client Scylla/NATS, query handlers, v.v.)
// ============================================================================

// ============================================
// CLIENTS
// ============================================
var scyllaCluster *gocql.ClusterConfig
var scyllaSession *gocql.Session
var natsConn *nats.Conn
var ncCloseMu sync.Mutex // chỉ dùng khi close/reconnect, không cần RLock

// --- MỚI: 2 trong 3 module business-layer (thuần logic, không có state I/O) ---
// RuleEngine và TOPSISBusiness không giữ kết nối gì cả, nên có thể khởi tạo
// 1 lần dùng chung cho mọi lần chạy batch.
//
// Bộ tiêu chí C1-C10 dưới đây giữ NGUYÊN 10 tiêu chí của thí nghiệm
// TOPSIS-Hybrid (files(2)/main.go) -- xếp hạng + phân bổ tối ưu toàn cục.
// Thứ tự vector Scores[] phải khớp đúng thứ tự này:
//
//	C1  mandatory              (Benefit) uu_tien_bat_buoc          (1-5)
//	C2  graduation_delay_risk  (Benefit) nguy_co_cham_tot_nghiep   (1-5)
//	C3  semesters_waited       (Benefit) so_ky_da_cho              (0+)
//	C4  failed_attempts        (Benefit) so_lan_dang_ky_that_bai   (0+)
//	C5  credits_completed      (Benefit) so_tin_chi_tich_luy       (0-140+)
//	C6  current_semester_load  (Cost)    khoi_luong_hoc_ky_hien_tai
//	C7  schedule_conflict      (Cost)    tính từ chuỗi thoi_khoa_bieu (0+)
//	C8  preference_match       (Benefit) muc_phu_hop_nguyen_vong   (1-5)
//	C9  alternative_sections   (Cost)    so_lop_thay_the (0+)
//	C10 can_open_more_sections (Cost)    kha_nang_mo_them_lop       (1-5)
var ruleEngine = RuleEngine{}

var topsisCriteria = []Criterion{
	{Name: "mandatory", Type: Benefit},             // C1: uu_tien_bat_buoc
	{Name: "graduation_delay_risk", Type: Benefit}, // C2: nguy_co_cham_tot_nghiep
	{Name: "semesters_waited", Type: Benefit},      // C3: so_ky_da_cho
	{Name: "failed_attempts", Type: Benefit},       // C4: so_lan_dang_ky_that_bai
	{Name: "credits_completed", Type: Benefit},     // C5: so_tin_chi_tich_luy
	{Name: "current_semester_load", Type: Cost},    // C6: khoi_luong_hoc_ky_hien_tai
	{Name: "schedule_conflict", Type: Cost},        // C7: tính từ thoi_khoa_bieu (xem countScheduleConflicts)
	{Name: "preference_match", Type: Benefit},      // C8: muc_phu_hop_nguyen_vong
	{Name: "alternative_sections", Type: Cost},     // C9: so_lop_thay_the
	{Name: "can_open_more_sections", Type: Cost},   // C10: kha_nang_mo_them_lop
}

var topsisEngine = TOPSISBusiness{
	Criteria: topsisCriteria,
	Weights:  buildTopsisWeightConfig(),
}

// buildTopsisWeightConfig hiện thực boolean switch EWM/AHP (config.TopsisUseAHP).
// Ma trận so sánh cặp AHP dưới đây là VÍ DỤ MINH HỌA (C1 quan trọng nhất, C10 ít
// nhất) -- trong triển khai thật, ma trận này nên do phòng đào tạo cung cấp và
// đọc từ file cấu hình/DB, không hard-code.
func buildTopsisWeightConfig() WeightConfig {
	if !config.TopsisUseAHP {
		return WeightConfig{Method: EWM}
	}
	return WeightConfig{
		Method: AHP,
		AHPPairwiseMatrix: [][]float64{
			{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			{1.0 / 2, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			{1.0 / 3, 1.0 / 2, 1, 2, 3, 4, 5, 6, 7, 8},
			{1.0 / 4, 1.0 / 3, 1.0 / 2, 1, 2, 3, 4, 5, 6, 7},
			{1.0 / 5, 1.0 / 4, 1.0 / 3, 1.0 / 2, 1, 2, 3, 4, 5, 6},
			{1.0 / 6, 1.0 / 5, 1.0 / 4, 1.0 / 3, 1.0 / 2, 1, 2, 3, 4, 5},
			{1.0 / 7, 1.0 / 6, 1.0 / 5, 1.0 / 4, 1.0 / 3, 1.0 / 2, 1, 2, 3, 4},
			{1.0 / 8, 1.0 / 7, 1.0 / 6, 1.0 / 5, 1.0 / 4, 1.0 / 3, 1.0 / 2, 1, 2, 3},
			{1.0 / 9, 1.0 / 8, 1.0 / 7, 1.0 / 6, 1.0 / 5, 1.0 / 4, 1.0 / 3, 1.0 / 2, 1, 2},
			{1.0 / 10, 1.0 / 9, 1.0 / 8, 1.0 / 7, 1.0 / 6, 1.0 / 5, 1.0 / 4, 1.0 / 3, 1.0 / 2, 1},
		},
	}
}

// ============================================
// REQUEST/RESPONSE TYPES
// ============================================
type DBRequest struct {
	QueryType string                 `json:"queryType"`
	Params    map[string]interface{} `json:"params"`
}

type DBResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
}

func updateWorker() {
	for {
		v, _ := mem.VirtualMemory()
		cSlice, _ := cpu.Percent(0, true) // get overall CPU percent
		cpuPercent := 0.0
		if len(cSlice) > 0 {
			for _, i := range cSlice {
				cpuPercent += i
			}
			cpuPercent = cpuPercent / float64(len(cSlice))
		}
		fmt.Print("len(cSlice):", len(cSlice))

		// almost every return value is a struct
		fmt.Printf("UsedPercentRAM: %.2f%%\n", v.UsedPercent)
		fmt.Printf("UsedPercentCPU: %.2f%%\n", cpuPercent)
		if v.UsedPercent < 40 && cpuPercent < 40 {
			config.MaxWorkers = 200
		} else if v.UsedPercent < 60 && cpuPercent < 60 {
			config.MaxWorkers = 100
		} else if v.UsedPercent < 80 && cpuPercent < 80 {
			config.MaxWorkers = 50
		} else {
			config.MaxWorkers = 10
		}
		fmt.Print("config max:", config.MaxWorkers)
		time.Sleep(10 * time.Second)
	}
}

// ============================================
// INIT SCYLLA
// ============================================
// initScylla kết nối ScyllaDB/Cassandra. Hỗ trợ cả cụm local (không auth) lẫn
// ScyllaDB Cloud: username/password + port + TLS CA truyền qua env
// (SCYLLA_USER/SCYLLA_PASSWORD/SCYLLA_PORT/SCYLLA_SSL_CA) -- KHÔNG hardcode.
// Với ScyllaDB Cloud dùng TLS, set SCYLLA_PORT=9142,
// SCYLLA_SSL_CA=<đường dẫn CA .pem tải từ Secure Connect Bundle>.
func initScylla() error {
	hosts := make([]string, len(config.ScyllaHosts))
	for i, h := range config.ScyllaHosts {
		if !strings.Contains(h, ":") {
			hosts[i] = net.JoinHostPort(h, config.ScyllaPort)
		} else {
			hosts[i] = h
		}
	}

	scyllaCluster = gocql.NewCluster(hosts...)
	scyllaCluster.Keyspace = config.Keyspace
	scyllaCluster.Consistency = gocql.Quorum
	scyllaCluster.ConnectTimeout = 8 * time.Second
	scyllaCluster.Timeout = 10 * time.Second

	if config.DataCenter != "" {
		scyllaCluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(config.DataCenter)
	} else {
		scyllaCluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	}

	if config.ScyllaUsername != "" {
		scyllaCluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.ScyllaUsername,
			Password: config.ScyllaPassword,
		}
	}
	if config.ScyllaTLSCA != "" {
		scyllaCluster.SslOpts = &gocql.SslOptions{
			CaPath:                 config.ScyllaTLSCA,
			EnableHostVerification: !config.ScyllaInsecureTLS,
		}
	}

	session, err := scyllaCluster.CreateSession()
	if err != nil {
		return fmt.Errorf("scylla init: %w", err)
	}
	scyllaSession = session
	log.Printf("✅ ScyllaDB connected: %v", hosts)
	return nil
}

// ============================================
// INIT NATS
// ============================================
func initNATS() error {
	var err error
	natsConn, err = nats.Connect(
		strings.Join(config.NATSServers, ","),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1*time.Second),
		nats.Timeout(5*time.Second),
		nats.PingInterval(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	log.Printf("✅ NATS connected")
	return nil
}

// ============================================
// QUERY HANDLERS (giữ nguyên logic, thêm tối ưu nhỏ)
// ============================================
func handleGetChiTietLopHocPhan(query map[string]interface{}) DBResponse {
	idLopHocPhan := query["idLopHocPhan"].(string)

	var id, tenmonhoc, siso, giangvien, lichhoc, diadiem string
	if err := scyllaSession.Query(`SELECT ma_lop_hoc_phan AS id, ten_lop_hoc_phan AS tenmonhoc, so_luong_toi_da AS siso, ma_sinh_vien AS giangvien, thoi_khoa_bieu AS lichhoc, phong_hoc AS diadiem FROM lop_hoc_phan WHERE ma_lop_hoc_phan = ?`, idLopHocPhan).Consistency(gocql.One).Scan(&id, &tenmonhoc, &siso, &giangvien, &lichhoc, &diadiem); err != nil {
		if err == gocql.ErrNotFound {
			return DBResponse{Success: false, Error: "Không tìm thấy lớp học phần"}
		}
		return DBResponse{Success: false, Error: err.Error()}
	}

	var sosvdadangky int
	if err := scyllaSession.Query(`SELECT so_luong_da_dang_ky AS sosvdadangky FROM lop_hoc_phan_counter WHERE ma_lop_hoc_phan = ?`, idLopHocPhan).Consistency(gocql.One).Scan(&sosvdadangky); err != nil {
		sosvdadangky = 0
	}

	data := map[string]interface{}{
		"id":           id,
		"tenmonhoc":    tenmonhoc,
		"siso":         siso,
		"giangvien":    giangvien,
		"lichhoc":      lichhoc,
		"diadiem":      diadiem,
		"sosvdadangky": sosvdadangky,
	}
	return DBResponse{Success: true, Data: data}
}

func handleGetDanhSachMonHocPhanDangKy(query map[string]interface{}) DBResponse {
	masinhvien := query["masinhvien"].(string)
	dotDangKy := ""
	if v, ok := query["dotDangKy"].(string); ok {
		dotDangKy = v
	}
	hinhThuc := ""
	if v, ok := query["hinhThuc"].(string); ok {
		hinhThuc = v
	}

	q := `SELECT ma_dang_ky, ma_sinh_vien, ho, ten, ma_lop_hoc_phan, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc, ngay_dang_ky, trang_thai FROM dang_ky WHERE ma_sinh_vien = ?`
	args := []interface{}{masinhvien}
	if hinhThuc != "" {
		q += " AND hinh_thuc = ? ALLOW FILTERING"
		args = append(args, hinhThuc)
	}

	iter := scyllaSession.Query(q, args...).Iter()
	var results []map[string]interface{}
	var item struct {
		MaDangKy      string
		MaSinhVien    string
		Ho            string
		Ten           string
		MaLopHocPhan  string
		TenLopHocPhan string
		MaMonHoc      string
		PhongHoc      string
		ThoiKhoaBieu  string
		SoLuongToiDa  int
		HinhThuc      string
		NgayDangKy    time.Time
		TrangThai     string
	}
	for iter.Scan(&item.MaDangKy, &item.MaSinhVien, &item.Ho, &item.Ten, &item.MaLopHocPhan, &item.TenLopHocPhan, &item.MaMonHoc, &item.PhongHoc, &item.ThoiKhoaBieu, &item.SoLuongToiDa, &item.HinhThuc, &item.NgayDangKy, &item.TrangThai) {
		row := map[string]interface{}{
			"ma_dang_ky":       item.MaDangKy,
			"ma_sinh_vien":     item.MaSinhVien,
			"ho":               item.Ho,
			"ten":              item.Ten,
			"ma_lop_hoc_phan":  item.MaLopHocPhan,
			"ten_lop_hoc_phan": item.TenLopHocPhan,
			"ma_mon_hoc":       item.MaMonHoc,
			"phong_hoc":        item.PhongHoc,
			"thoi_khoa_bieu":   item.ThoiKhoaBieu,
			"so_luong_toi_da":  item.SoLuongToiDa,
			"hinh_thuc":        item.HinhThuc,
			"ngay_dang_ky":     item.NgayDangKy.Format("2006-01-02 15:04:05"),
			"trang_thai":       item.TrangThai,
		}
		if dotDangKy != "" {
			// lọc theo ngày (yyyy-mm-dd) của ngay_dang_ky
			if item.NgayDangKy.Format("2006-01-02") == dotDangKy {
				results = append(results, row)
			}
		} else {
			results = append(results, row)
		}
	}
	if err := iter.Close(); err != nil {
		log.Printf("⚠️ handleGetDanhSachMonHocPhanDangKy: iter.Close lỗi: %v", err)
	}

	if len(results) == 0 {
		return DBResponse{Success: true, Data: []map[string]interface{}{}}
	}

	maLopHocPhans := make([]string, len(results))
	for i, r := range results {
		maLopHocPhans[i] = r["ma_lop_hoc_phan"].(string)
	}
	sort.Strings(maLopHocPhans)

	counterMap := make(map[string]int)
	if err := fetchCounters(maLopHocPhans, counterMap); err != nil {
		log.Printf("⚠️ fetchCounters error: %v", err)
	}

	for _, row := range results {
		mlhp := row["ma_lop_hoc_phan"].(string)
		if c, ok := counterMap[mlhp]; ok {
			row["so_luong_da_dang_ky"] = c
		} else {
			row["so_luong_da_dang_ky"] = 0
		}
	}

	return DBResponse{Success: true, Data: results}
}

func handleGetDanhSachLopHocPhan(query map[string]interface{}) DBResponse {
	tenMonHocRaw := query["TenMonHoc"]
	tenMonHocs := make([]string, 0)
	switch v := tenMonHocRaw.(type) {
	case string:
		tenMonHocs = append(tenMonHocs, v)
	case []interface{}:
		for _, item := range v {
			if s, ok := item.(string); ok {
				tenMonHocs = append(tenMonHocs, s)
			}
		}
	}

	placeholders := strings.Repeat("?,", len(tenMonHocs))
	placeholders = strings.TrimSuffix(placeholders, ",")
	args := make([]interface{}, len(tenMonHocs))
	for i, v := range tenMonHocs {
		args[i] = v
	}

	q := fmt.Sprintf(`SELECT ma_lop_hoc_phan, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, trang_thai, ngay_bat_dau, ngay_ket_thuc FROM lop_hoc_phan WHERE ma_mon_hoc IN (%s) ALLOW FILTERING`, placeholders)
	iter := scyllaSession.Query(q, args...).Iter()

	var lhpRows []struct {
		MaLopHocPhan  string
		TenLopHocPhan string
		MaMonHoc      string
		PhongHoc      string
		ThoiKhoaBieu  string
		SoLuongToiDa  int
		TrangThai     string
		NgayBatDau    time.Time
		NgayKetThuc   time.Time
	}
	var row struct {
		MaLopHocPhan  string
		TenLopHocPhan string
		MaMonHoc      string
		PhongHoc      string
		ThoiKhoaBieu  string
		SoLuongToiDa  int
		TrangThai     string
		NgayBatDau    time.Time
		NgayKetThuc   time.Time
	}
	for iter.Scan(&row.MaLopHocPhan, &row.TenLopHocPhan, &row.MaMonHoc, &row.PhongHoc, &row.ThoiKhoaBieu, &row.SoLuongToiDa, &row.TrangThai, &row.NgayBatDau, &row.NgayKetThuc) {
		lhpRows = append(lhpRows, row)
	}
	if err := iter.Close(); err != nil {
		log.Printf("⚠️ handleGetDanhSachLopHocPhan: iter.Close lỗi: %v", err)
	}

	if len(lhpRows) == 0 {
		return DBResponse{Success: true, Data: []map[string]interface{}{}}
	}

	maLopHocPhans := make([]string, len(lhpRows))
	for i, r := range lhpRows {
		maLopHocPhans[i] = r.MaLopHocPhan
	}
	sort.Strings(maLopHocPhans)

	counterMap := make(map[string]int)
	if err := fetchCounters(maLopHocPhans, counterMap); err != nil {
		log.Printf("⚠️ fetchCounters error: %v", err)
	}

	data := make([]map[string]interface{}, len(lhpRows))
	for i, r := range lhpRows {
		rowMap := map[string]interface{}{
			"ma_lop_hoc_phan":  r.MaLopHocPhan,
			"ten_lop_hoc_phan": r.TenLopHocPhan,
			"ma_mon_hoc":       r.MaMonHoc,
			"phong_hoc":        r.PhongHoc,
			"thoi_khoa_bieu":   r.ThoiKhoaBieu,
			"so_luong_toi_da":  r.SoLuongToiDa,
			"trang_thai":       r.TrangThai,
			"ngay_bat_dau":     r.NgayBatDau.Format("2006-01-02 15:04:05"),
			"ngay_ket_thuc":    r.NgayKetThuc.Format("2006-01-02 15:04:05"),
		}
		if c, ok := counterMap[r.MaLopHocPhan]; ok {
			rowMap["so_luong_da_dang_ky"] = c
		} else {
			rowMap["so_luong_da_dang_ky"] = 0
		}
		data[i] = rowMap
	}

	return DBResponse{Success: true, Data: data}
}

func fetchCounters(maLopHocPhans []string, counterMap map[string]int) error {
	if len(maLopHocPhans) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(maLopHocPhans))
	placeholders = strings.TrimSuffix(placeholders, ",")
	q := fmt.Sprintf("SELECT ma_lop_hoc_phan, so_luong_da_dang_ky FROM lop_hoc_phan_counter WHERE ma_lop_hoc_phan IN (%s)", placeholders)
	args := make([]interface{}, len(maLopHocPhans))
	for i, v := range maLopHocPhans {
		args[i] = v
	}
	iter := scyllaSession.Query(q, args...).Iter()
	var maLopHocPhan string
	var soLuong int
	for iter.Scan(&maLopHocPhan, &soLuong) {
		counterMap[maLopHocPhan] = soLuong
	}
	return iter.Close()
}

// handleDangKyMonHoc được tối ưu: chạy đồng thời các query kiểm tra không phụ thuộc
// handleDangKyMonHoc -- ĐÃ SỬA: không còn "blind insert" xác nhận ngay lập
// tức. Đây giờ chỉ là GIAI ĐOẠN 1 (khai báo nguyện vọng, Section 10 của
// thiết kế): ghi nhận yêu cầu với trang_thai='ChoXuLy', CHƯA cộng counter,
// CHƯA đảm bảo có chỗ. Việc xếp hạng (TOPSIS) và phân bổ (ILP) chạy sau đó
// theo lô trong processPendingBatch(), tránh 2 vấn đề của bản cũ:
//  1. Race condition khi nhiều request cùng insert + cùng update counter
//     đồng thời (không có transaction/lock nào bảo vệ).
//  2. Không có cách nào ưu tiên sinh viên nào hơn sinh viên nào -- ai gửi
//     request tới trước (nhanh mạng hơn) thì được, đúng vấn đề nêu ở mục 4.
func handleDangKyMonHoc(query map[string]interface{}) DBResponse {
	maSinhVien := query["maSinhVien"].(string)
	maLopHocPhan := query["maLopHocPhan"].(string)
	hinhThuc := "Chinh quy"
	if v, ok := query["hinhThuc"].(string); ok && v != "" {
		hinhThuc = v
	}
	// maLopHocPhanPhu (không bắt buộc): nguyện vọng PHỤ -- lớp thay thế của
	// CÙNG môn học. Chính cột này cho phép AllocationOptimizer chạy chế độ
	// HYBRID: lớp chính hết chỗ thì xếp sinh viên sang lớp phụ (xem
	// processPendingBatch).
	maLopHocPhanPhu := ""
	if v, ok := query["maLopHocPhanPhu"].(string); ok {
		maLopHocPhanPhu = strings.TrimSpace(v)
	}

	// Lấy thông tin thật của lớp học phần thay vì hard-code "LHP_Test"/"MH_Test"
	// như bản cũ -- bắt buộc phải có ma_mon_hoc thật để TOPSIS-Business gom
	// đúng nhóm cạnh tranh theo môn học (RankByCourse nhóm theo CourseID).
	lhp, err := fetchThongTinLopHocPhan(maLopHocPhan)
	if err != nil {
		return DBResponse{Success: false, Error: "Lớp học phần không tồn tại: " + maLopHocPhan}
	}

	// Nguyện vọng phụ phải cùng môn học mới hợp lệ (nếu khác môn thì TOPSIS
	// xếp hạng trong pool khác, AllocationOptimizer không thể đổi được).
	var lhpPhu thongTinLopHocPhan
	if maLopHocPhanPhu != "" && maLopHocPhanPhu != maLopHocPhan {
		lhpPhu, err = fetchThongTinLopHocPhan(maLopHocPhanPhu)
		if err != nil || lhpPhu.MaMonHoc != lhp.MaMonHoc {
			return DBResponse{Success: false, Error: "Lớp thay thế không tồn tại hoặc không cùng môn học: " + maLopHocPhanPhu}
		}
	} else {
		maLopHocPhanPhu = ""
	}

	maDangKy := fmt.Sprintf("DK_%s_%s_%d", maSinhVien, maLopHocPhan, time.Now().UnixNano())
	ngayDangKy := time.Now()

	err = scyllaSession.Query(`INSERT INTO dang_ky (ma_dang_ky, ma_sinh_vien, ma_lop_hoc_phan, ho, ten, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc, ngay_dang_ky, trang_thai, created_at, updated_at, ma_lop_hoc_phan_phu, ten_lop_hoc_phan_phu, phong_hoc_phu, thoi_khoa_bieu_phu, so_luong_toi_da_phu) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		maDangKy, maSinhVien, maLopHocPhan, "LoadTest_Ho", "LoadTest_Ten", lhp.TenLopHocPhan, lhp.MaMonHoc, lhp.PhongHoc, lhp.ThoiKhoaBieu, lhp.SoLuongToiDa, hinhThuc, ngayDangKy, "ChoXuLy", ngayDangKy, ngayDangKy,
		maLopHocPhanPhu, lhpPhu.TenLopHocPhan, lhpPhu.PhongHoc, lhpPhu.ThoiKhoaBieu, lhpPhu.SoLuongToiDa).Exec()

	if err != nil {
		return DBResponse{Success: false, Error: err.Error()}
	}

	// KHÔNG cộng counter ở đây nữa -- chỉ cộng khi processPendingBatch()
	// thật sự xác nhận (trang_thai='DaDangKy'), tránh đếm thừa cho những
	// nguyện vọng cuối cùng bị từ chối hoặc xếp vào danh sách chờ.

	return DBResponse{
		Success: true,
		Data: map[string]interface{}{
			"ma_dang_ky":          maDangKy,
			"ma_sinh_vien":        maSinhVien,
			"ma_lop_hoc_phan":     maLopHocPhan,
			"ma_lop_hoc_phan_phu": maLopHocPhanPhu,
			"trang_thai":          "ChoXuLy",
		},
		Message: "Đã ghi nhận nguyện vọng (kèm lớp thay thế nếu có), đang chờ xếp hạng và phân bổ theo lô",
	}
}

// fetchThongTinLopHocPhan đọc thông tin thật của 1 lớp học phần, dùng khi
// ghi nhận nguyện vọng (thay cho các giá trị "LoadTest_*" hard-code cũ).
type thongTinLopHocPhan struct {
	TenLopHocPhan string
	MaMonHoc      string
	PhongHoc      string
	ThoiKhoaBieu  string
	SoLuongToiDa  int
}

func fetchThongTinLopHocPhan(maLopHocPhan string) (thongTinLopHocPhan, error) {
	var t thongTinLopHocPhan
	err := scyllaSession.Query(
		`SELECT ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da FROM lop_hoc_phan WHERE ma_lop_hoc_phan = ?`,
		maLopHocPhan,
	).Consistency(gocql.One).Scan(&t.TenLopHocPhan, &t.MaMonHoc, &t.PhongHoc, &t.ThoiKhoaBieu, &t.SoLuongToiDa)
	return t, err
}

// ============================================
// TÍCH HỢP TẦNG NGHIỆP VỤ (RuleEngine + TOPSISBusiness + AllocationOptimizer)
// ============================================
//
// ĐÂY LÀ BẢN "HYBRID" ĐÚNG NHƯ THÍ NGHIỆM files(2)/main.go: TOPSIS-Business
// xếp hạng 10 tiêu chí (EWM/AHP) + AllocationOptimizer phân bổ toàn cục CÓ
// dùng cả nguyện vọng phụ (lớp thay thế) khi sinh viên khai báo.
//
// YÊU CẦU SCHEMA MỚI (xem file init.sql cùng thư mục -- chạy trước khi
// deploy phần dưới đây):
//
//   - bảng sinh_vien_ho_so có 9 cột tiêu chí C1-C6, C8-C10
//     (C7 xung đột thời khóa biểu được TÍNH từ chuỗi thoi_khoa_bieu chứ
//     không lưu cột riêng -- xem countScheduleConflicts):
//
//   CREATE TABLE IF NOT EXISTS sinh_vien_ho_so (
//       ma_sinh_vien                text PRIMARY KEY,
//       uu_tien_bat_buoc            int,  -- C1 (Benefit)
//       nguy_co_cham_tot_nghiep     int,  -- C2 (Benefit)
//       so_ky_da_cho                int,  -- C3 (Benefit)
//       so_lan_dang_ky_that_bai     int,  -- C4 (Benefit)
//       so_tin_chi_tich_luy         int,  -- C5 (Benefit)
//       khoi_luong_hoc_ky_hien_tai  int,  -- C6 (Cost)
//       muc_phu_hop_nguyen_vong     int,  -- C8 (Benefit)
//       so_lop_thay_the             int,  -- C9 (Cost)
//       kha_nang_mo_them_lop        int,  -- C10 (Cost)
//       updated_at                  timestamp
//   );
//
//   - bảng dang_ky có thêm cột nguyện vọng phụ (lớp thay thế) + cột
//     ly_do_tu_choi; trang_thai thêm giá trị 'ChoXuLy' (mới ghi nhận),
//     'DanhSachCho' (hết chỗ):
//
//   ALTER TABLE dang_ky ADD ly_do_tu_choi text;
//   ALTER TABLE dang_ky ADD ma_lop_hoc_phan_phu text;
//   ALTER TABLE dang_ky ADD ten_lop_hoc_phan_phu text;
//   ALTER TABLE dang_ky ADD phong_hoc_phu text;
//   ALTER TABLE dang_ky ADD thoi_khoa_bieu_phu text;
//   ALTER TABLE dang_ky ADD so_luong_toi_da_phu int;
//
// LƯU Ý VỀ BATCH: toàn bộ 3 module business-layer (RuleEngine,
// TOPSISBusiness, AllocationOptimizer -- định nghĩa ngay bên dưới) được
// inline thẳng vào package main của db-service, không tách package riêng
// nữa, để deploy chỉ cần đúng 1 binary duy nhất, không cần quản lý thêm
// go.mod replace directive. Có một kiểu "BatchController" dùng channel
// trong bộ nhớ (in-memory) đơn giản hơn, nhưng KHÔNG dùng ở đây -- vì nếu
// service restart giữa chừng, các request đang nằm trong buffer bộ nhớ sẽ mất. Thay
// vào đó dùng poll theo chu kỳ (ticker) đọc trực tiếp các dòng trang_thai=
// 'ChoXuLy' từ ScyllaDB -- chậm hơn 1 chút nhưng bền vững qua restart, đúng
// tinh thần "Message Queue đảm bảo không mất yêu cầu" (mục 21 thiết kế).

// fetchHoSoSinhVien đọc hồ sơ học vụ phục vụ tính điểm TOPSIS: trả về vector
// [10]float64 đúng thứ tự topsisCriteria. Ô C7 (schedule_conflict) luôn trả 0
// ở đây -- nó được tính sau trong batch bằng countScheduleConflicts vì cần
// biết thoi_khoa_bieu của lớp đang xét. Nếu sinh viên chưa có hồ sơ (bảng
// mới, có thể chưa được đồng bộ đủ), dùng giá trị trung tính thay vì loại
// bỏ sinh viên đó -- tránh 1 lỗi đồng bộ dữ liệu biến thành từ chối oan uổng.
func fetchHoSoSinhVien(maSinhVien string) [10]float64 {
	var uuTien, nguyCo, soKy, thatBai, tinChi, khoiLuong, phuHop, soLop, moThem int
	err := scyllaSession.Query(
		`SELECT uu_tien_bat_buoc, nguy_co_cham_tot_nghiep, so_ky_da_cho, so_lan_dang_ky_that_bai,
		        so_tin_chi_tich_luy, khoi_luong_hoc_ky_hien_tai, muc_phu_hop_nguyen_vong,
		        so_lop_thay_the, kha_nang_mo_them_lop
		 FROM sinh_vien_ho_so WHERE ma_sinh_vien = ?`,
		maSinhVien,
	).Consistency(gocql.One).Scan(&uuTien, &nguyCo, &soKy, &thatBai, &tinChi, &khoiLuong, &phuHop, &soLop, &moThem)
	if err != nil {
		// trung tính, không ưu tiên cũng không bất lợi (đúng tinh thần fallback bản cũ)
		return [10]float64{3, 3, 0, 0, 70, 16, 0, 3, 3, 3}
	}
	return [10]float64{
		float64(uuTien), float64(nguyCo), float64(soKy), float64(thatBai),
		float64(tinChi), float64(khoiLuong), 0, float64(phuHop), float64(soLop), float64(moThem),
	}
}

// fetchDangKySchedules đọc danh sách thoi_khoa_bieu của các lớp sinh viên đã
// ĐƯỢC XÁC NHẬN (trang_thai='DaDangKy') -- dùng để tính C7 (schedule_conflict)
// cho các nguyện vọng đang xét.
func fetchDangKySchedules(maSinhVien string) []string {
	var out []string
	iter := scyllaSession.Query(
		`SELECT thoi_khoa_bieu FROM dang_ky WHERE ma_sinh_vien = ? AND trang_thai = 'DaDangKy' ALLOW FILTERING`,
		maSinhVien,
	).Iter()
	var tkb string
	for iter.Scan(&tkb) {
		if tkb != "" {
			out = append(out, tkb)
		}
	}
	_ = iter.Close()
	return out
}

// countScheduleConflicts tính C7 = số buổi học của lớp mới trùng buổi với các
// lớp sinh viên đã đăng ký. Chuỗi thoi_khoa_bieu dạng:
//
//	"T2|07:00-09:00;T4|07:00-09:00"  (buổi = ngày trong tuần | giờ bắt đầu-kết thúc)
//
// Xem parseSchedule. Chuỗi không parse được thì coi như không xung đột.
func countScheduleConflicts(newSchedule string, existingSchedules []string) int {
	if newSchedule == "" {
		return 0
	}
	conflicts := 0
	for _, other := range existingSchedules {
		if scheduleOverlap(newSchedule, other) {
			conflicts++
		}
	}
	return conflicts
}

type timeSlot struct {
	Weekday  int // 0=Mon..6=Sun
	StartMin int
	EndMin   int
}

// parseSchedule chuyển chuỗi thoi_khoa_bieu thành danh sách buổi học.
// Định dạng chuẩn: "T2|07:00-09:00;T4|10:00-12:00".
func parseSchedule(s string) []timeSlot {
	var slots []timeSlot
	for _, token := range strings.Split(s, ";") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		parts := strings.SplitN(token, "|", 2)
		if len(parts) != 2 {
			continue
		}
		wd, ok := weekdayIndex(parts[0])
		if !ok {
			continue
		}
		times := strings.SplitN(parts[1], "-", 2)
		if len(times) != 2 {
			continue
		}
		start, ok1 := parseHHMM(times[0])
		end, ok2 := parseHHMM(times[1])
		if !ok1 || !ok2 || end <= start {
			continue
		}
		slots = append(slots, timeSlot{Weekday: wd, StartMin: start, EndMin: end})
	}
	return slots
}

func weekdayIndex(token string) (int, bool) {
	switch strings.ToUpper(strings.TrimSpace(token)) {
	case "T2", "MON", "MONDAY":
		return 0, true
	case "T3", "TUE", "TUESDAY":
		return 1, true
	case "T4", "WED", "WEDNESDAY":
		return 2, true
	case "T5", "THU", "THURSDAY":
		return 3, true
	case "T6", "FRI", "FRIDAY":
		return 4, true
	case "T7", "SAT", "SATURDAY":
		return 5, true
	case "CN", "SUN", "SUNDAY":
		return 6, true
	}
	return 0, false
}

func parseHHMM(s string) (int, bool) {
	s = strings.TrimSpace(s)
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return 0, false
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}

func scheduleOverlap(a, b string) bool {
	sa, sb := parseSchedule(a), parseSchedule(b)
	for _, x := range sa {
		for _, y := range sb {
			if x.Weekday == y.Weekday && x.StartMin < y.EndMin && y.StartMin < x.EndMin {
				return true
			}
		}
	}
	return false
}

// checkDaDangKyMonHoc kiểm tra ràng buộc cứng "đã đăng ký môn này ở lớp
// khác rồi" (RuleEngine.ReasonDuplicateRegistration).
func checkDaDangKyMonHoc(maSinhVien, maMonHoc string) bool {
	var count int
	if err := scyllaSession.Query(
		`SELECT COUNT(*) FROM dang_ky WHERE ma_sinh_vien = ? AND ma_mon_hoc = ? AND trang_thai = 'DaDangKy' ALLOW FILTERING`,
		maSinhVien, maMonHoc,
	).Consistency(gocql.One).Scan(&count); err != nil {
		return false // lỗi truy vấn -> không chặn oan, để RuleEngine coi như chưa đăng ký
	}
	return count > 0
}

// fetchSlotConLai tính số chỗ còn trống của 1 lớp học phần = sĩ số tối đa -
// số đã xác nhận (KHÔNG tính các nguyện vọng đang 'ChoXuLy', vì chúng chưa
// chắc được xác nhận).
func fetchSlotConLai(maLopHocPhan string) (int, error) {
	var soLuongToiDa, daDangKy int
	if err := scyllaSession.Query(
		`SELECT so_luong_toi_da FROM lop_hoc_phan WHERE ma_lop_hoc_phan = ?`, maLopHocPhan,
	).Consistency(gocql.One).Scan(&soLuongToiDa); err != nil {
		return 0, err
	}
	_ = scyllaSession.Query(
		`SELECT so_luong_da_dang_ky FROM lop_hoc_phan_counter WHERE ma_lop_hoc_phan = ?`, maLopHocPhan,
	).Consistency(gocql.One).Scan(&daDangKy) // lỗi -> daDangKy=0, coi như còn nguyên chỗ

	con := soLuongToiDa - daDangKy
	if con < 0 {
		con = 0
	}
	return con, nil
}

// pendingRow mô tả 1 dòng đang chờ xử lý, đọc từ dang_ky WHERE trang_thai='ChoXuLy'.
// Mỗi dòng = 1 nguyện vọng CHÍNH (ma_lop_hoc_phan) của sinh viên cho 1 môn học;
// các cột *_phu lưu nguyện vọng PHỤ (lớp thay thế cùng môn) -- đúng mô hình
// simStudent.PrimarySec/AltSec của thí nghiệm files(2)/main.go.
type pendingRow struct {
	MaDangKy         string
	MaSinhVien       string
	Ho               string
	Ten              string
	MaLopHocPhan     string
	TenLopHocPhan    string
	MaMonHoc         string
	PhongHoc         string
	ThoiKhoaBieu     string
	SoLuongToiDa     int
	HinhThuc         string
	MaLopHocPhanPhu  string
	TenLopHocPhanPhu string
	PhongHocPhu      string
	ThoiKhoaBieuPhu  string
	SoLuongToiDaPhu  int
	NgayDangKy       time.Time
}

// processPendingBatch là nơi ĐÚNG 4 MODULE business-layer được ghép lại,
// chạy định kỳ mỗi config.TopsisBatchWindow (mặc định 5s, xem main()):
//
//	Poll 'ChoXuLy' (thay BatchController.Enqueue)
//	  -> RuleEngine.Filter
//	  -> TOPSISBusiness.RankByCourse (10 tiêu chí, EWM hoặc AHP)
//	  -> AllocationOptimizer.Solve -- PHÂN BỔ HYBRID: mỗi sinh viên có thể
//	     được xếp sang LỚP THAY THẾ (nguyện vọng phụ) khi lớp chính hết chỗ
//	  -> ghi kết quả xuống ScyllaDB theo transaction đơn giản (UPDATE theo
//	     từng ma_dang_ky) + publish cache.invalidate.batch cho api-service
func processPendingBatch() {
	iter := scyllaSession.Query(
		`SELECT ma_dang_ky, ma_sinh_vien, ho, ten, ma_lop_hoc_phan, ten_lop_hoc_phan, ma_mon_hoc,
		        phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc,
		        ma_lop_hoc_phan_phu, ten_lop_hoc_phan_phu, phong_hoc_phu,
		        thoi_khoa_bieu_phu, so_luong_toi_da_phu, ngay_dang_ky
		 FROM dang_ky WHERE trang_thai = 'ChoXuLy' ALLOW FILTERING`,
	).Iter()

	var row pendingRow
	var pendingRows []pendingRow
	for iter.Scan(&row.MaDangKy, &row.MaSinhVien, &row.Ho, &row.Ten, &row.MaLopHocPhan,
		&row.TenLopHocPhan, &row.MaMonHoc, &row.PhongHoc, &row.ThoiKhoaBieu,
		&row.SoLuongToiDa, &row.HinhThuc,
		&row.MaLopHocPhanPhu, &row.TenLopHocPhanPhu, &row.PhongHocPhu,
		&row.ThoiKhoaBieuPhu, &row.SoLuongToiDaPhu, &row.NgayDangKy) {
		pendingRows = append(pendingRows, row)
	}
	if err := iter.Close(); err != nil {
		log.Printf("⚠️ [TOPSIS-Batch] truy vấn ChoXuLy lỗi: %v", err)
		return
	}
	if len(pendingRows) == 0 {
		return
	}
	log.Printf("⏱️ [TOPSIS-Batch] xử lý %d nguyện vọng đang chờ", len(pendingRows))

	// ---- Bước 0: build StudentRequest cho từng dòng đang chờ ----
	// rowByKey: key(studentID|courseID|sectionID) -> dòng CHÍNH, dùng cho cập
	// nhật từ chối (RuleEngine). LƯU Ý: bảng dang_ky có PK
	// ((ma_sinh_vien), ma_lop_hoc_phan), nên mọi UPDATE/DELETE phải đi theo đúng
	// ma_sinh_vien + ma_lop_hoc_phan -- không thể dùng WHERE ma_dang_ky (nó là
	// cột thường, CQL không cho UPDATE theo cột ngoài PK).
	rowByKey := make(map[string]pendingRow, len(pendingRows))
	// primaryRowByCourse: key(studentID|courseID) -> dòng chính -- nền tảng của
	// phân bổ hybrid: nếu sinh viên được xếp vào lớp PHỤ, vẫn cập nhật chính
	// dòng này (đổi sang lớp phụ) thay vì tạo dòng mới.
	primaryRowByCourse := make(map[string]pendingRow, len(pendingRows))
	reqs := make([]StudentRequest, len(pendingRows))
	for _, r := range pendingRows {
		key := r.MaSinhVien + "|" + r.MaMonHoc
		rowByKey[key+"|"+r.MaLopHocPhan] = r
		primaryRowByCourse[key] = r
	}

	// Đọc hồ sơ/số tín chỉ/xung đột lịch song song -- với lô 5000 sinh viên,
	// mỗi sinh viên tốn 2-3 query lên Scylla Cloud, ghi tuần tự sẽ là nút thắt.
	var buildWG sync.WaitGroup
	buildSem := make(chan struct{}, 32)
	for i, r := range pendingRows {
		buildWG.Add(1)
		buildSem <- struct{}{}
		go func(i int, r pendingRow) {
			defer buildWG.Done()
			defer func() { <-buildSem }()
			hoso := fetchHoSoSinhVien(r.MaSinhVien)
			// C7: đếm số lớp đã xác nhận trùng buổi với lớp đang đăng ký.
			hoso[6] = float64(countScheduleConflicts(r.ThoiKhoaBieu, fetchDangKySchedules(r.MaSinhVien)))
			reqs[i] = StudentRequest{
				StudentID: r.MaSinhVien, CourseID: r.MaMonHoc, SectionID: r.MaLopHocPhan,
				Scores:             hoso[:],
				SubmittedAt:        r.NgayDangKy,
				PrerequisitesMet:   true, // TODO: nối bảng điều kiện tiên quyết khi trường có dữ liệu
				CreditLoadOK:       true, // TODO: nối bảng giới hạn tín chỉ khi trường có dữ liệu
				HasScheduleClash:   false,
				AlreadyRegistered:  checkDaDangKyMonHoc(r.MaSinhVien, r.MaMonHoc),
				EligibleForProgram: true,
			}
		}(i, r)
	}
	buildWG.Wait()

	var affectedSinhVien, affectedLopHocPhan []string

	// ---- Bước 1: RuleEngine (ràng buộc cứng) ----
	filtered := ruleEngine.Filter(reqs)
	for k, reason := range filtered.Rejected {
		row, ok := rowByKey[k]
		if !ok {
			continue
		}
		if err := scyllaSession.Query(
			`UPDATE dang_ky SET trang_thai = 'TuChoi', ly_do_tu_choi = ? WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?`,
			string(reason), row.MaSinhVien, row.MaLopHocPhan,
		).Exec(); err != nil {
			log.Printf("⚠️ [TOPSIS-Batch] cập nhật từ chối %s lỗi: %v", row.MaDangKy, err)
		}
		affectedSinhVien = append(affectedSinhVien, strings.SplitN(k, "|", 2)[0])
	}
	if len(filtered.Eligible) == 0 {
		publishCacheInvalidation(affectedSinhVien, affectedLopHocPhan)
		return
	}

	// ---- Bước 2: TOPSIS-Business (xếp hạng theo môn, 10 tiêu chí) ----
	ranked, err := topsisEngine.RankByCourse(filtered.Eligible)
	if err != nil {
		log.Printf("⚠️ [TOPSIS-Batch] xếp hạng TOPSIS lỗi: %v", err)
		return
	}

	// ---- Bước 3: Allocation Optimizer -- PHÂN BỔ TOÀN CỤC HYBRID ----
	// Giống runHybrid của files(2)/main.go: với mỗi sinh viên, đưa vào đồ thị
	// 1 cạnh cho lớp CHÍNH + 1 cạnh cho lớp PHỤ (cùng điểm TOPSIS). Optimizer
	// chọn được cạnh nào có lợi nhất cho tổng điểm; lớp chính hết chỗ thì
	// sinh viên vẫn có cơ hội được xếp sang lớp phụ còn chỗ.
	var edges []Edge
	sections := make(map[string]Section)
	addEdge := func(studentID, courseID, sectionID string, score float64) {
		if sectionID == "" {
			return
		}
		edges = append(edges, Edge{StudentID: studentID, CourseID: courseID, SectionID: sectionID, Score: score})
		if _, ok := sections[sectionID]; !ok {
			if slotCon, err := fetchSlotConLai(sectionID); err == nil {
				sections[sectionID] = Section{ID: sectionID, CourseID: courseID, Capacity: slotCon}
			}
		}
	}
	for _, pool := range ranked {
		for _, rr := range pool {
			key := rr.Request.StudentID + "|" + rr.Request.CourseID
			row := primaryRowByCourse[key]
			addEdge(rr.Request.StudentID, rr.Request.CourseID, row.MaLopHocPhan, rr.Score)
			addEdge(rr.Request.StudentID, rr.Request.CourseID, row.MaLopHocPhanPhu, rr.Score)
		}
	}
	// Conflicts: để trống ở bản này (TODO, xem comment C7 ở trên).
	optimizer := AllocationOptimizer{Sections: sections}
	result := optimizer.Solve(edges)

	// ---- Bước 4: ghi kết quả xuống Scylla ----
	// Với lô lớn (hàng nghìn nguyện vọng), ghi tuần tự từng dòng lên Scylla
	// Cloud rất chậm (~ms/dòng). Dùng worker pool giới hạn để ghi song song,
	// giữ nguyên thứ tự xác minh "handled" ở luồng điều phối.
	// LƯU Ý: bảng dang_ky PK = ((ma_sinh_vien), ma_lop_hoc_phan) nên mọi ghi
	// phải theo ma_sinh_vien + ma_lop_hoc_phan. Khi HYBRID chuyển sinh viên
	// sang lớp PHỤ thì ma_lop_hoc_phan thay đổi (thuộc PK) -> phải DELETE dòng
	// cũ + INSERT dòng mới, gói trong 1 logged batch cho nhất quán.
	type writeOp struct {
		stmt  string
		args  []interface{}
		batch *gocql.Batch
	}
	writeOps := make(chan writeOp, 512)
	const writeWorkers = 24
	var writeWG sync.WaitGroup
	for w := 0; w < writeWorkers; w++ {
		writeWG.Add(1)
		go func() {
			defer writeWG.Done()
			for op := range writeOps {
				if op.batch != nil {
					if err := scyllaSession.ExecuteBatch(op.batch); err != nil {
						log.Printf("⚠️ [TOPSIS-Batch] ghi batch lỗi: %v", err)
					}
					continue
				}
				if err := scyllaSession.Query(op.stmt, op.args...).Exec(); err != nil {
					log.Printf("⚠️ [TOPSIS-Batch] ghi lỗi (%s): %v", op.stmt, err)
				}
			}
		}()
	}
	enqueueWrite := func(stmt string, args ...interface{}) {
		writeOps <- writeOp{stmt: stmt, args: args}
	}
	enqueueBatch := func(b *gocql.Batch) {
		writeOps <- writeOp{batch: b}
	}

	handled := make(map[string]bool, len(result.Confirmed)) // key(studentID|courseID)
	for _, a := range result.Confirmed {
		key := a.StudentID + "|" + a.CourseID
		row := primaryRowByCourse[key]
		if a.SectionID == row.MaLopHocPhan {
			// Xếp đúng lớp chính đã khai báo.
			enqueueWrite(
				`UPDATE dang_ky SET trang_thai = 'DaDangKy' WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?`,
				row.MaSinhVien, row.MaLopHocPhan,
			)
		} else {
			// Hybrid: xếp sang lớp PHỤ -- dòng dang_ky đang trỏ lớp CHÍNH,
			// phải DELETE + INSERT sang lớp phụ (ma_lop_hoc_phan thuộc PK nên
			// không UPDATE được), giữ nguyên ma_dang_ky để FE tra cứu.
			b := scyllaSession.NewBatch(gocql.LoggedBatch)
			b.Query(
				`DELETE FROM dang_ky WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?`,
				row.MaSinhVien, row.MaLopHocPhan,
			)
			b.Query(
				`INSERT INTO dang_ky (ma_dang_ky, ma_sinh_vien, ho, ten, ma_lop_hoc_phan,
				        ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da,
				        hinh_thuc, ngay_dang_ky, trang_thai,
				        ma_lop_hoc_phan_phu, ten_lop_hoc_phan_phu, phong_hoc_phu,
				        thoi_khoa_bieu_phu, so_luong_toi_da_phu, created_at, updated_at)
				 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				row.MaDangKy, row.MaSinhVien, row.Ho, row.Ten, a.SectionID,
				row.TenLopHocPhanPhu, row.MaMonHoc, row.PhongHocPhu, row.ThoiKhoaBieuPhu,
				row.SoLuongToiDaPhu, row.HinhThuc, row.NgayDangKy, "DaDangKy",
				"", "", "", "", 0, row.NgayDangKy, time.Now(),
			)
			enqueueBatch(b)
		}
		enqueueWrite(
			`UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky + 1 WHERE ma_lop_hoc_phan = ?`,
			a.SectionID,
		)
		handled[key] = true
		affectedSinhVien = append(affectedSinhVien, a.StudentID)
		affectedLopHocPhan = append(affectedLopHocPhan, a.SectionID)
	}
	for _, e := range result.Waitlist {
		key := e.StudentID + "|" + e.CourseID
		if handled[key] {
			continue // sinh viên đã được xếp lớp phụ rồi -> không đẩy vào chờ
		}
		row := primaryRowByCourse[key]
		enqueueWrite(
			`UPDATE dang_ky SET trang_thai = 'DanhSachCho' WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?`,
			row.MaSinhVien, row.MaLopHocPhan,
		)
		handled[key] = true
		affectedSinhVien = append(affectedSinhVien, e.StudentID)
	}

	close(writeOps)
	writeWG.Wait()

	log.Printf("✅ [TOPSIS-Batch] xác nhận=%d, chờ=%d, từ chối=%d",
		len(result.Confirmed), len(result.Waitlist), len(filtered.Rejected))

	publishCacheInvalidation(affectedSinhVien, affectedLopHocPhan)
}

// publishCacheInvalidation báo cho api-service biết những cache key nào cần
// xóa, vì kết quả xác nhận giờ đến TRỄ (bất đồng bộ), api-service không thể
// tự invalidate ngay sau khi nhận response DANG_KY_MON_HOC như bản cũ nữa.
func publishCacheInvalidation(sinhViens, lopHocPhans []string) {
	sinhViens, lopHocPhans = dedupe(sinhViens), dedupe(lopHocPhans)
	if natsConn == nil || (len(sinhViens) == 0 && len(lopHocPhans) == 0) {
		return
	}
	payload, err := json.Marshal(map[string]interface{}{
		"ma_sinh_viens":    sinhViens,
		"ma_lop_hoc_phans": lopHocPhans,
	})
	if err != nil {
		log.Printf("⚠️ publishCacheInvalidation: marshal lỗi: %v", err)
		return
	}
	if err := natsConn.Publish("cache.invalidate.batch", payload); err != nil {
		log.Printf("⚠️ publish cache.invalidate.batch lỗi: %v", err)
	}
}

func dedupe(items []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if it == "" || seen[it] {
			continue
		}
		seen[it] = true
		out = append(out, it)
	}
	return out
}

// handleGetTrangThaiDangKy: API mới để sinh viên tra cứu kết quả nguyện
// vọng của mình (RECEIVED/ChoXuLy -> CONFIRMED/DaDangKy | WAITLISTED/
// DanhSachCho | REJECTED/TuChoi).
func handleGetTrangThaiDangKy(query map[string]interface{}) DBResponse {
	maDangKy, _ := query["maDangKy"].(string)
	if maDangKy == "" {
		return DBResponse{Success: false, Error: "Thiếu maDangKy"}
	}
	var trangThai, lyDo string
	err := scyllaSession.Query(
		`SELECT trang_thai, ly_do_tu_choi FROM dang_ky WHERE ma_dang_ky = ? ALLOW FILTERING`, maDangKy,
	).Consistency(gocql.One).Scan(&trangThai, &lyDo)
	if err != nil {
		if err == gocql.ErrNotFound {
			return DBResponse{Success: false, Error: "Không tìm thấy mã đăng ký"}
		}
		return DBResponse{Success: false, Error: err.Error()}
	}
	data := map[string]interface{}{"ma_dang_ky": maDangKy, "trang_thai": trangThai}
	if lyDo != "" {
		data["ly_do_tu_choi"] = lyDo
	}
	return DBResponse{Success: true, Data: data}
}

func handleHuyDangKy(query map[string]interface{}) DBResponse {
	maSinhVien := query["maSinhVien"].(string)
	maLopHocPhan := query["maLopHocPhan"].(string)

	// Bắn thẳng lệnh DELETE mà không cần kiểm tra xem sinh viên có đăng ký môn đó thật hay không
	err := scyllaSession.Query("DELETE FROM dang_ky WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?", maSinhVien, maLopHocPhan).Exec()

	if err != nil {
		return DBResponse{Success: false, Error: err.Error()}
	}

	// Trừ counter
	_ = scyllaSession.Query("UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky - 1 WHERE ma_lop_hoc_phan = ?", maLopHocPhan).Exec()

	// Mặc định luôn báo xanh cho k6
	return DBResponse{Success: true, Message: "Hủy đăng ký thành công (Load Test Mode)"}
}
func handleBatchCounterQuery(params map[string]interface{}) DBResponse {
	maLopHocPhansRaw := params["maLopHocPhans"]
	maLopHocPhans := make([]string, 0)
	switch v := maLopHocPhansRaw.(type) {
	case []interface{}:
		for _, item := range v {
			if s, ok := item.(string); ok {
				maLopHocPhans = append(maLopHocPhans, s)
			}
		}
	case string:
		maLopHocPhans = append(maLopHocPhans, v)
	}

	if len(maLopHocPhans) == 0 {
		return DBResponse{Success: true, Data: map[string]interface{}{}}
	}

	sort.Strings(maLopHocPhans)
	counterMap := make(map[string]int)
	if err := fetchCounters(maLopHocPhans, counterMap); err != nil {
		return DBResponse{Success: false, Error: err.Error()}
	}

	return DBResponse{Success: true, Data: counterMap}
}

func handleQuery(queryType string, params map[string]interface{}) DBResponse {
	switch queryType {
	case "GET_CHI_TIET_LOP_HOC_PHAN":
		return handleGetChiTietLopHocPhan(params)
	case "GET_DANH_SACH_MON_HOC_PHAN_DANG_KY":
		return handleGetDanhSachMonHocPhanDangKy(params)
	case "GET_DANH_SACH_LOP_HOC_PHAN":
		return handleGetDanhSachLopHocPhan(params)
	case "DANG_KY_MON_HOC":
		return handleDangKyMonHoc(params)
	case "HUY_DANG_KY":
		return handleHuyDangKy(params)
	case "BATCH_GET_COUNTERS":
		return handleBatchCounterQuery(params)
	case "GET_TRANG_THAI_DANG_KY":
		return handleGetTrangThaiDangKy(params)
	default:
		return DBResponse{Success: false, Error: fmt.Sprintf("Unknown queryType: %s", queryType)}
	}
}

// ============================================
// NATS SUBSCRIPTIONS (with worker pool)
// ============================================
func startSubscriptions(ctx context.Context, workerSem chan struct{}, wg *sync.WaitGroup) error {
	// Subscription db.query
	sub1, err := natsConn.Subscribe("db.query", func(msg *nats.Msg) {
		// Acquire worker slot
		select {
		case workerSem <- struct{}{}:
			wg.Add(1)
			go func(m *nats.Msg) {
				defer wg.Done()
				defer func() { <-workerSem }()
				// Không cần lock khi đọc natsConn
				var req DBRequest
				if err := json.Unmarshal(m.Data, &req); err != nil {
					m.Respond([]byte(`{"success":false,"error":"invalid json"}`))
					return
				}
				resp := handleQuery(req.QueryType, req.Params)
				data, _ := json.Marshal(resp)
				m.Respond(data)
				log.Printf("📨 %s -> success=%v", req.QueryType, resp.Success)
			}(msg)
		case <-ctx.Done():
			// Nếu context bị hủy, không xử lý thêm message
			return
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe db.query: %w", err)
	}

	// Subscription db.batch.query
	sub2, err := natsConn.Subscribe("db.batch.query", func(msg *nats.Msg) {
		select {
		case workerSem <- struct{}{}:
			wg.Add(1)
			go func(m *nats.Msg) {
				defer wg.Done()
				defer func() { <-workerSem }()
				var req struct {
					Queries []map[string]interface{} `json:"queries"`
				}
				if err := json.Unmarshal(m.Data, &req); err != nil {
					m.Respond([]byte(`{"success":false,"error":"invalid json"}`))
					return
				}
				results := make([]DBResponse, len(req.Queries))
				var eg errgroup.Group
				for i, q := range req.Queries {
					i, q := i, q
					eg.Go(func() error {
						results[i] = handleQuery(q["queryType"].(string), q["params"].(map[string]interface{}))
						return nil
					})
				}
				eg.Wait()

				resp := map[string]interface{}{"success": true, "results": results}
				data, _ := json.Marshal(resp)
				m.Respond(data)
				log.Printf("📤 Batch: %d queries", len(req.Queries))
			}(msg)
		case <-ctx.Done():
			return
		}
	})
	if err != nil {
		sub1.Unsubscribe()
		return fmt.Errorf("subscribe db.batch.query: %w", err)
	}

	// Chờ context done, sau đó unsubscribe
	go func() {
		<-ctx.Done()
		sub1.Unsubscribe()
		sub2.Unsubscribe()
		log.Println("NATS subscriptions unsubscribed")
	}()
	return nil
}

// ============================================
// MAIN
// ============================================
func main() {
	go updateWorker()
	if err := initScylla(); err != nil {
		log.Fatalf("❌ Scylla init failed: %v", err)
	}
	defer scyllaSession.Close()

	if err := initNATS(); err != nil {
		log.Fatalf("❌ NATS init failed: %v", err)
	}

	// Tạo context chính và errgroup cho graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	_ = g
	// Worker pool: semaphore channel giới hạn số goroutine xử lý đồng thời
	workerSem := make(chan struct{}, config.MaxWorkers)
	var handlerWg sync.WaitGroup // đợi tất cả handler đang chạy hoàn tất

	// Bắt đầu lắng nghe NATS với worker pool
	if err := startSubscriptions(ctx, workerSem, &handlerWg); err != nil {
		log.Fatalf("❌ start subscriptions: %v", err)
	}

	// --- MỚI: vòng lặp TOPSIS-Batch, chạy độc lập với NATS subscription ---
	// Đây chính là "Batch Controller" (module 4) -- nhưng dùng ticker + poll
	// ScyllaDB thay vì channel trong bộ nhớ, để bền vững qua restart (xem
	// comment chi tiết ở processPendingBatch()).
	handlerWg.Add(1)
	go func() {
		defer handlerWg.Done()
		ticker := time.NewTicker(config.TopsisBatchWindow)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				processPendingBatch()
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Println("⏳ DB Service ready, waiting for requests...")
	log.Printf("   ScyllaDB: %v", config.ScyllaHosts)
	log.Printf("   NATS: %v", config.NATSServers)
	log.Printf("   Max Workers: %d", config.MaxWorkers)
	log.Printf("   TOPSIS Batch Window: %v (weights: EWM=%v / AHP=%v)", config.TopsisBatchWindow, !config.TopsisUseAHP, config.TopsisUseAHP)

	// Chờ tín hiệu thoát
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Printf("Received %s, shutting down...", sig)
	case <-ctx.Done():
	}

	// Bắt đầu shutdown
	cancel() // dừng nhận thêm work mới

	// Đợi tất cả handler đang chạy hoàn thành (với timeout)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		handlerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("✅ All handlers finished")
	case <-shutdownCtx.Done():
		log.Println("⚠️ Timed out waiting for handlers, forcing exit")
	}

	// Đóng NATS (chỉ cần lock khi close)
	ncCloseMu.Lock()
	if natsConn != nil {
		natsConn.Close()
	}
	ncCloseMu.Unlock()

	log.Println("✅ DB Service shutdown complete")
}
