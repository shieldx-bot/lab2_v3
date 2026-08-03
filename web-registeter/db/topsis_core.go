package main

import (
	"errors"
	"math"
	"sort"
	"time"
)

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
