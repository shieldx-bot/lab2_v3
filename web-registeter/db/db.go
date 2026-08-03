package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
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

	// --- MỚI: cấu hình cho tầng nghiệp vụ (business-layer TOPSIS) ---
	TopsisBatchWindow time.Duration // chu kỳ quét bảng dang_ky trạng thái 'ChoXuLy' (Section 7.2)
	TopsisUseAHP      bool          // true = dùng AHP (ma trận so sánh cặp), false = dùng EWM (mặc định)
}

func LoadConfig() Config {
	// 1. Đọc các chuỗi phân tách bằng dấu phẩy thành []string
	scyllaHosts := strings.Split(getEnv("SCYLLA_HOSTS", "192.168.0.2:9042"), ",")
	natsServers := strings.Split(getEnv("NATS_SERVERS", "nats://192.168.0.2:4222"), ",")

	topsisBatchWindow, err := time.ParseDuration(getEnv("TOPSIS_BATCH_WINDOW", "5s"))
	if err != nil {
		topsisBatchWindow = 5 * time.Second
	}

	return Config{
		ScyllaHosts:       scyllaHosts,
		Keyspace:          getEnv("KEYSPACE", "my_keyspace"),
		NATSServers:       natsServers,
		DataCenter:        getEnv("DATA_CENTER", "datacenter1"),
		MaxWorkers:        50,
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
// 1 lần dùng chung cho mọi lần chạy batch. Bộ tiêu chí C1-C5 tương ứng đúng
// 5 cột mới cần thêm vào bảng sinh_vien_ho_so (xem comment ở processPendingBatch).
var ruleEngine = RuleEngine{}

var topsisCriteria = []Criterion{
	{Name: "mandatory", Type: Benefit},             // C1: uu_tien_bat_buoc
	{Name: "graduation_delay_risk", Type: Benefit}, // C2: nguy_co_cham_tot_nghiep
	{Name: "semesters_waited", Type: Benefit},      // C3: so_ky_da_cho
	{Name: "alternative_sections", Type: Cost},     // C4: so_lop_thay_the
	{Name: "schedule_conflict", Type: Cost},        // C5: chưa wire, luôn = 0 (TODO)
}

var topsisEngine = TOPSISBusiness{
	Criteria: topsisCriteria,
	Weights:  buildTopsisWeightConfig(),
}

// buildTopsisWeightConfig hiện thực boolean switch EWM/AHP (config.TopsisUseAHP).
// Ma trận so sánh cặp AHP dưới đây là VÍ DỤ MINH HỌA (C1 quan trọng nhất, C5 ít
// nhất) -- trong triển khai thật, ma trận này nên do phòng đào tạo cung cấp và
// đọc từ file cấu hình/DB, không hard-code.
func buildTopsisWeightConfig() WeightConfig {
	if !config.TopsisUseAHP {
		return WeightConfig{Method: EWM}
	}
	return WeightConfig{
		Method: AHP,
		AHPPairwiseMatrix: [][]float64{
			{1, 2, 3, 4, 5},
			{1.0 / 2, 1, 2, 3, 4},
			{1.0 / 3, 1.0 / 2, 1, 2, 3},
			{1.0 / 4, 1.0 / 3, 1.0 / 2, 1, 2},
			{1.0 / 5, 1.0 / 4, 1.0 / 3, 1.0 / 2, 1},
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
func initScylla() error {
	scyllaCluster = gocql.NewCluster(config.ScyllaHosts...)
	scyllaCluster.Keyspace = config.Keyspace
	scyllaCluster.Consistency = gocql.Quorum
	scyllaCluster.ConnectTimeout = 5 * time.Second
	scyllaCluster.Timeout = 10 * time.Second
	scyllaCluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()

	session, err := scyllaCluster.CreateSession()
	if err != nil {
		return fmt.Errorf("scylla init: %w", err)
	}
	scyllaSession = session
	log.Printf("✅ ScyllaDB connected: %v", config.ScyllaHosts)
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
		NgayDangKy    string
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
			"ngay_dang_ky":     item.NgayDangKy,
			"trang_thai":       item.TrangThai,
		}
		if dotDangKy != "" {
			if dt, err := time.Parse(time.RFC3339, item.NgayDangKy); err == nil {
				if dt.Format("2006-01-02") == dotDangKy {
					results = append(results, row)
				}
			}
		} else {
			results = append(results, row)
		}
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
		NgayBatDau    string
		NgayKetThuc   string
	}
	var row struct {
		MaLopHocPhan  string
		TenLopHocPhan string
		MaMonHoc      string
		PhongHoc      string
		ThoiKhoaBieu  string
		SoLuongToiDa  int
		TrangThai     string
		NgayBatDau    string
		NgayKetThuc   string
	}
	for iter.Scan(&row.MaLopHocPhan, &row.TenLopHocPhan, &row.MaMonHoc, &row.PhongHoc, &row.ThoiKhoaBieu, &row.SoLuongToiDa, &row.TrangThai, &row.NgayBatDau, &row.NgayKetThuc) {
		lhpRows = append(lhpRows, row)
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
			"ngay_bat_dau":     r.NgayBatDau,
			"ngay_ket_thuc":    r.NgayKetThuc,
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

	// Lấy thông tin thật của lớp học phần thay vì hard-code "LHP_Test"/"MH_Test"
	// như bản cũ -- bắt buộc phải có ma_mon_hoc thật để TOPSIS-Business gom
	// đúng nhóm cạnh tranh theo môn học (RankByCourse nhóm theo CourseID).
	lhp, err := fetchThongTinLopHocPhan(maLopHocPhan)
	if err != nil {
		return DBResponse{Success: false, Error: "Lớp học phần không tồn tại: " + maLopHocPhan}
	}

	maDangKy := fmt.Sprintf("DK_%s_%s_%d", maSinhVien, maLopHocPhan, time.Now().UnixNano())
	ngayDangKy := time.Now()

	err = scyllaSession.Query(`INSERT INTO dang_ky (ma_dang_ky, ma_sinh_vien, ma_lop_hoc_phan, ho, ten, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc, ngay_dang_ky, trang_thai, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		maDangKy, maSinhVien, maLopHocPhan, "LoadTest_Ho", "LoadTest_Ten", lhp.TenLopHocPhan, lhp.MaMonHoc, lhp.PhongHoc, lhp.ThoiKhoaBieu, lhp.SoLuongToiDa, hinhThuc, ngayDangKy, "ChoXuLy", ngayDangKy, ngayDangKy).Exec()

	if err != nil {
		return DBResponse{Success: false, Error: err.Error()}
	}

	// KHÔNG cộng counter ở đây nữa -- chỉ cộng khi processPendingBatch()
	// thật sự xác nhận (trang_thai='DaDangKy'), tránh đếm thừa cho những
	// nguyện vọng cuối cùng bị từ chối hoặc xếp vào danh sách chờ.

	return DBResponse{
		Success: true,
		Data: map[string]interface{}{
			"ma_dang_ky":      maDangKy,
			"ma_sinh_vien":    maSinhVien,
			"ma_lop_hoc_phan": maLopHocPhan,
			"trang_thai":      "ChoXuLy",
		},
		Message: "Đã ghi nhận nguyện vọng, đang chờ xếp hạng và phân bổ theo lô",
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
// YÊU CẦU SCHEMA MỚI (chạy migration trước khi deploy phần dưới đây):
//
//   ALTER TABLE dang_ky ADD ly_do_tu_choi text;
//   -- trang_thai có thêm 2 giá trị mới: 'ChoXuLy' (mặc định lúc mới đăng ký,
//   -- thay cho 'DaDangKy' cũ), 'DanhSachCho' (xếp hàng chờ khi hết chỗ).
//
//   CREATE TABLE IF NOT EXISTS sinh_vien_ho_so (
//       ma_sinh_vien text PRIMARY KEY,
//       uu_tien_bat_buoc int,        -- C1: mức bắt buộc của môn (1-5)
//       nguy_co_cham_tot_nghiep int, -- C2: nguy cơ chậm tốt nghiệp (1-5)
//       so_ky_da_cho int,            -- C3: số học kỳ đã chờ học phần này
//       so_lop_thay_the int          -- C4: số lớp thay thế khả dụng (chi phí)
//   );
//
// Tiêu chí C5 (xung đột thời khóa biểu) CHƯA wire trong bản này -- luôn để
// 0 (không xung đột) cho mọi request. Việc phát hiện xung đột cần so khớp
// chuỗi thoi_khoa_bieu giữa các lớp sinh viên đã đăng ký, để lại TODO.
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

// fetchHoSoSinhVien đọc hồ sơ học vụ phục vụ tính điểm TOPSIS. Nếu sinh viên
// chưa có hồ sơ (bảng mới, có thể chưa được đồng bộ đủ), dùng giá trị trung
// tính thay vì loại bỏ sinh viên đó -- tránh 1 lỗi đồng bộ dữ liệu biến
// thành từ chối oan uổng.
func fetchHoSoSinhVien(maSinhVien string) [4]float64 {
	var uuTien, nguyCo, soKy, soLop int
	err := scyllaSession.Query(
		`SELECT uu_tien_bat_buoc, nguy_co_cham_tot_nghiep, so_ky_da_cho, so_lop_thay_the FROM sinh_vien_ho_so WHERE ma_sinh_vien = ?`,
		maSinhVien,
	).Consistency(gocql.One).Scan(&uuTien, &nguyCo, &soKy, &soLop)
	if err != nil {
		return [4]float64{3, 3, 0, 5} // trung tính, không ưu tiên cũng không bất lợi
	}
	return [4]float64{float64(uuTien), float64(nguyCo), float64(soKy), float64(soLop)}
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
type pendingRow struct {
	MaDangKy     string
	MaSinhVien   string
	MaLopHocPhan string
	MaMonHoc     string
	NgayDangKy   time.Time
}

// processPendingBatch là nơi ĐÚNG 4 MODULE business-layer được ghép lại,
// chạy định kỳ mỗi config.TopsisBatchWindow (mặc định 5s, xem main()):
//
//	Poll 'ChoXuLy' (thay BatchController.Enqueue)
//	  -> RuleEngine.Filter
//	  -> TOPSISBusiness.RankByCourse (EWM hoặc AHP, xem buildTopsisWeightConfig)
//	  -> AllocationOptimizer.Solve
//	  -> ghi kết quả xuống ScyllaDB theo transaction đơn giản (UPDATE theo
//	     từng ma_dang_ky) + publish cache.invalidate.batch cho api-service
func processPendingBatch() {
	iter := scyllaSession.Query(
		`SELECT ma_dang_ky, ma_sinh_vien, ma_lop_hoc_phan, ma_mon_hoc, ngay_dang_ky FROM dang_ky WHERE trang_thai = 'ChoXuLy' ALLOW FILTERING`,
	).Iter()

	var row pendingRow
	var pendingRows []pendingRow
	for iter.Scan(&row.MaDangKy, &row.MaSinhVien, &row.MaLopHocPhan, &row.MaMonHoc, &row.NgayDangKy) {
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
	var reqs []StudentRequest
	trackingByKey := make(map[string]string) // key(studentID|courseID|sectionID) -> ma_dang_ky
	for _, r := range pendingRows {
		hoso := fetchHoSoSinhVien(r.MaSinhVien)
		reqs = append(reqs, StudentRequest{
			StudentID: r.MaSinhVien, CourseID: r.MaMonHoc, SectionID: r.MaLopHocPhan,
			Scores:             []float64{hoso[0], hoso[1], hoso[2], hoso[3], 0},
			SubmittedAt:        r.NgayDangKy,
			PrerequisitesMet:   true, // TODO: nối bảng điều kiện tiên quyết khi trường có dữ liệu
			CreditLoadOK:       true, // TODO: nối bảng giới hạn tín chỉ khi trường có dữ liệu
			HasScheduleClash:   false,
			AlreadyRegistered:  checkDaDangKyMonHoc(r.MaSinhVien, r.MaMonHoc),
			EligibleForProgram: true,
		})
		trackingByKey[r.MaSinhVien+"|"+r.MaMonHoc+"|"+r.MaLopHocPhan] = r.MaDangKy
	}

	var affectedSinhVien, affectedLopHocPhan []string

	// ---- Bước 1: RuleEngine (ràng buộc cứng) ----
	filtered := ruleEngine.Filter(reqs)
	for k, reason := range filtered.Rejected {
		maDangKy := trackingByKey[k]
		if err := scyllaSession.Query(
			`UPDATE dang_ky SET trang_thai = 'TuChoi', ly_do_tu_choi = ? WHERE ma_dang_ky = ?`,
			string(reason), maDangKy,
		).Exec(); err != nil {
			log.Printf("⚠️ [TOPSIS-Batch] cập nhật từ chối %s lỗi: %v", maDangKy, err)
		}
		affectedSinhVien = append(affectedSinhVien, strings.SplitN(k, "|", 2)[0])
	}
	if len(filtered.Eligible) == 0 {
		publishCacheInvalidation(affectedSinhVien, affectedLopHocPhan)
		return
	}

	// ---- Bước 2: TOPSIS-Business (xếp hạng theo môn) ----
	ranked, err := topsisEngine.RankByCourse(filtered.Eligible)
	if err != nil {
		log.Printf("⚠️ [TOPSIS-Batch] xếp hạng TOPSIS lỗi: %v", err)
		return
	}

	// ---- Bước 3: Allocation Optimizer (phân bổ toàn cục) ----
	var edges []Edge
	sections := make(map[string]Section)
	for _, pool := range ranked {
		for _, rr := range pool {
			edges = append(edges, Edge{
				StudentID: rr.Request.StudentID, CourseID: rr.Request.CourseID,
				SectionID: rr.Request.SectionID, Score: rr.Score,
			})
			if _, ok := sections[rr.Request.SectionID]; !ok {
				if slotCon, err := fetchSlotConLai(rr.Request.SectionID); err == nil {
					sections[rr.Request.SectionID] = Section{
						ID: rr.Request.SectionID, CourseID: rr.Request.CourseID, Capacity: slotCon,
					}
				}
			}
		}
	}
	optimizer := AllocationOptimizer{Sections: sections} // Conflicts: để trống ở bản này (TODO, xem comment C5 ở trên)
	result := optimizer.Solve(edges)

	// ---- Bước 4: ghi kết quả xuống Scylla ----
	for _, a := range result.Confirmed {
		maDangKy := trackingByKey[a.StudentID+"|"+a.CourseID+"|"+a.SectionID]
		if err := scyllaSession.Query(
			`UPDATE dang_ky SET trang_thai = 'DaDangKy' WHERE ma_dang_ky = ?`, maDangKy,
		).Exec(); err != nil {
			log.Printf("⚠️ [TOPSIS-Batch] xác nhận %s lỗi: %v", maDangKy, err)
			continue
		}
		_ = scyllaSession.Query(
			`UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky + 1 WHERE ma_lop_hoc_phan = ?`,
			a.SectionID,
		).Exec()
		affectedSinhVien = append(affectedSinhVien, a.StudentID)
		affectedLopHocPhan = append(affectedLopHocPhan, a.SectionID)
	}
	for _, e := range result.Waitlist {
		maDangKy := trackingByKey[e.StudentID+"|"+e.CourseID+"|"+e.SectionID]
		if err := scyllaSession.Query(
			`UPDATE dang_ky SET trang_thai = 'DanhSachCho' WHERE ma_dang_ky = ?`, maDangKy,
		).Exec(); err != nil {
			log.Printf("⚠️ [TOPSIS-Batch] cập nhật danh sách chờ %s lỗi: %v", maDangKy, err)
		}
		affectedSinhVien = append(affectedSinhVien, e.StudentID)
	}

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
