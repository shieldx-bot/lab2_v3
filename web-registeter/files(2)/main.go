// Chương trình mô phỏng thí nghiệm so sánh 4 chiến lược phân bổ đăng ký
// học phần, theo đúng thiết kế thí nghiệm: (1) FIFO, (2) Rule-based cố
// định, (3) TOPSIS đơn lẻ (chỉ xếp hạng, không phân bổ lại), (4) Hybrid
// (TOPSIS + AllocationOptimizer toàn cục).
//
// Dùng ĐÚNG code TOPSIS thật (topsis_core.go, trích nguyên văn từ db.go),
// không viết lại thuật toán riêng cho mô phỏng.
//
// Chạy:  go run .
// Kết quả: in bảng ra console + ghi 2 file CSV vào ./output/
package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------
// MÔ HÌNH DỮ LIỆU MÔ PHỎNG
// ---------------------------------------------------------------------

// simStudent gói 1 sinh viên + nguyện vọng chính (bắt buộc mọi chiến lược
// đều xét) và 1 nguyện vọng phụ (chỉ Hybrid mới tận dụng được, đại diện
// cho "chấp nhận chuyển lớp" -- Giai đoạn 1 của thiết kế gốc).
type simStudent struct {
	ID           string
	Urgent       bool // nhóm "cấp thiết cao" (~15%), dùng để đo priority fairness
	Scores       []float64
	CourseID     string
	PrimarySec   string
	AltSec       string // "" nếu course chỉ có 1 lớp
	SubmittedIdx int    // thứ tự gửi yêu cầu, dùng cho FIFO (độc lập với Urgent)
}

var criteria = []Criterion{
	{Name: "mandatory", Type: Benefit},
	{Name: "graduation_delay_risk", Type: Benefit},
	{Name: "semesters_waited", Type: Benefit},
	{Name: "failed_attempts", Type: Benefit},
	{Name: "credits_completed", Type: Benefit},
	{Name: "current_semester_load", Type: Cost},
	{Name: "schedule_conflict", Type: Cost},
	{Name: "preference_match", Type: Benefit},
	{Name: "alternative_sections", Type: Cost},
	{Name: "can_open_more_sections", Type: Cost},
}

const (
	numCourses        = 60
	sectionsPerCourse = 3
)

func randInt(rng *rand.Rand, min, max int) int { return min + rng.Intn(max-min+1) }

// ---------------------------------------------------------------------
// SINH DỮ LIỆU BẰNG KỸ THUẬT FOURIER (thay cho random rời rạc thuần túy)
// ---------------------------------------------------------------------
//
// Ý tưởng: thay vì rng.Perm / rng.Intn độc lập cho từng sinh viên (nhiễu
// trắng, không có cấu trúc), ta dùng tổng các hàm sin (chuỗi Fourier hữu
// hạn) để tạo:
//   (1) mật độ nộp đơn không đều theo thời gian (đợt cao điểm mở đăng ký,
//       nhắc nhở giữa kỳ, sát hạn chót) -- ảnh hưởng trực tiếp đến FIFO.
//   (2) các thuộc tính có tương quan mượt theo "vị trí cohort" của sinh
//       viên (i/N) thay vì random rời rạc từng người một -- thực tế hơn
//       vì sinh viên cùng khoá/cùng đợt thường có đặc điểm gần nhau.

type fourierComponent struct {
	Freq  float64 // số chu kỳ trên khoảng [0,1]
	Amp   float64 // biên độ
	Phase float64 // pha (rad)
}

// submissionWaveComponents mô phỏng 3 đợt cao điểm nộp đơn: ngay khi mở
// đăng ký (t≈0), đợt nhắc nhở giữa kỳ, và sát hạn chót (t≈1).
var submissionWaveComponents = []fourierComponent{
	{Freq: 1, Amp: 0.6, Phase: -math.Pi / 2},
	{Freq: 3, Amp: 0.3, Phase: math.Pi},
	{Freq: 6, Amp: 0.45, Phase: -math.Pi / 2},
}

// fourierIntensity trả về mật độ (>=0) tại thời điểm chuẩn hóa t in [0,1],
// tổng hợp từ các thành phần tần số trong components.
func fourierIntensity(t float64, components []fourierComponent) float64 {
	v := 1.0
	for _, c := range components {
		v += c.Amp * math.Sin(2*math.Pi*c.Freq*t+c.Phase)
	}
	if v < 0.05 {
		v = 0.05 // sàn nhỏ, tránh mật độ = 0 tuyệt đối
	}
	return v
}

// maxFourierIntensity ước lượng cận trên của fourierIntensity trên [0,1]
// bằng lấy mẫu dày, dùng làm envelope cho rejection sampling.
func maxFourierIntensity(components []fourierComponent) float64 {
	max := 0.0
	for i := 0; i <= 1000; i++ {
		t := float64(i) / 1000
		if v := fourierIntensity(t, components); v > max {
			max = v
		}
	}
	return max
}

// sampleSubmissionOrder sinh numStudents thời điểm nộp đơn t in [0,1] theo
// mật độ fourierIntensity (rejection sampling), rồi trả về thứ hạng nộp
// đơn (0..n-1) của từng sinh viên -- dùng làm SubmittedIdx cho FIFO.
func sampleSubmissionOrder(rng *rand.Rand, numStudents int, components []fourierComponent) []int {
	maxI := maxFourierIntensity(components)
	times := make([]float64, numStudents)
	for i := 0; i < numStudents; i++ {
		for {
			t := rng.Float64()
			y := rng.Float64() * maxI
			if y <= fourierIntensity(t, components) {
				times[i] = t
				break
			}
		}
	}
	idx := make([]int, numStudents)
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return times[idx[a]] < times[idx[b]] })
	order := make([]int, numStudents)
	for rank, i := range idx {
		order[i] = rank
	}
	return order
}

// Các thành phần Fourier riêng cho từng tiêu chí điểm số -- tần số/pha
// khác nhau để các tiêu chí không dao động đồng bộ với nhau (tránh tương
// quan giả tạo toàn phần giữa các cột dữ liệu, vốn có thể làm sai lệch
// trọng số EWM của TOPSIS).
var (
	mandatoryWave = []fourierComponent{{Freq: 2, Amp: 0.5, Phase: 0}, {Freq: 5, Amp: 0.25, Phase: math.Pi / 3}}
	creditsWave   = []fourierComponent{{Freq: 1, Amp: 0.7, Phase: -math.Pi / 2}, {Freq: 4, Amp: 0.2, Phase: math.Pi}}
	loadWave      = []fourierComponent{{Freq: 3, Amp: 0.4, Phase: math.Pi / 4}}
	prefMatchWave = []fourierComponent{{Freq: 2, Amp: 0.5, Phase: math.Pi}}
	altSecWave    = []fourierComponent{{Freq: 4, Amp: 0.5, Phase: 0}}
	canOpenWave   = []fourierComponent{{Freq: 2, Amp: 0.45, Phase: -math.Pi / 3}}
)

// fourierScore sinh một giá trị nguyên trong [min,max] bằng cách: (a)
// tổng hợp chuỗi Fourier tại vị trí cohort t để tạo xu hướng mượt theo
// nhóm sinh viên, (b) cộng nhiễu Gaussian nhỏ để các sinh viên trong cùng
// nhóm không bị trùng giá trị tuyệt đối, (c) lượng tử hóa và kẹp về range.
func fourierScore(rng *rand.Rand, t float64, comps []fourierComponent, min, max int, noiseSigma float64) int {
	v := 1.0
	for _, c := range comps {
		v += c.Amp * math.Sin(2*math.Pi*c.Freq*t+c.Phase)
	}
	norm := v / 2 // v dao động quanh 1 (biên độ tổng < 1) -> chuẩn hóa về ~[0,1]
	if norm < 0 {
		norm = 0
	}
	if norm > 1 {
		norm = 1
	}
	val := float64(min) + norm*float64(max-min)
	val += rng.NormFloat64() * noiseSigma
	r := int(math.Round(val))
	if r < min {
		r = min
	}
	if r > max {
		r = max
	}
	return r
}

// generateData sinh numStudents sinh viên cạnh tranh vào numCourses môn
// học (mỗi môn sectionsPerCourse lớp), với tổng sức chứa được tính ngược
// từ overloadRatio = demand / capacity.
func generateData(seed int64, numStudents int, overloadRatio float64) ([]simStudent, map[string]Section, map[string]string) {
	rng := rand.New(rand.NewSource(seed))

	totalSections := numCourses * sectionsPerCourse
	totalCapacity := int(float64(numStudents) / overloadRatio)
	if totalCapacity < totalSections {
		totalCapacity = totalSections // đảm bảo mỗi lớp có ít nhất 1 chỗ
	}
	capPerSection := totalCapacity / totalSections

	sections := make(map[string]Section, totalSections)
	courseOfSection := make(map[string]string, totalSections)
	courseSections := make([][]string, numCourses)
	for c := 0; c < numCourses; c++ {
		courseID := fmt.Sprintf("MH%03d", c)
		for s := 0; s < sectionsPerCourse; s++ {
			secID := fmt.Sprintf("%s-L%d", courseID, s)
			sections[secID] = Section{ID: secID, CourseID: courseID, Capacity: capPerSection}
			courseOfSection[secID] = courseID
			courseSections[c] = append(courseSections[c], secID)
		}
	}

	students := make([]simStudent, numStudents)
	// Thứ tự gửi giờ được sinh theo mật độ Fourier (đợt cao điểm mở đăng
	// ký / nhắc nhở / sát hạn) thay vì hoán vị đều rng.Perm, độc lập với
	// mức độ cấp thiết.
	order := sampleSubmissionOrder(rng, numStudents, submissionWaveComponents)
	for i := 0; i < numStudents; i++ {
		urgent := rng.Float64() < 0.15
		// t: vị trí "cohort" của sinh viên trong quần thể (0..1) -- dùng
		// làm biến pha cho các chuỗi Fourier, tạo tương quan mượt theo
		// nhóm thay vì random rời rạc độc lập từng người.
		t := float64(i) / float64(numStudents)
		var mandatory, gradRisk, semWaited, failedAttempts, credits, load, prefMatch, altSecCount, canOpen int
		mandatory = fourierScore(rng, t, mandatoryWave, 1, 5, 0.4)
		// gradRisk/semWaited/failedAttempts giữ nguyên logic rẽ nhánh theo
		// urgent, vì đây chính là các tiêu chí định nghĩa "cấp thiết" --
		// không làm mượt bằng Fourier để tránh xoá nhòa ranh giới nhóm.
		if urgent {
			gradRisk = randInt(rng, 4, 5)
			semWaited = randInt(rng, 2, 5)
			failedAttempts = randInt(rng, 1, 3)
		} else {
			gradRisk = randInt(rng, 1, 3)
			semWaited = randInt(rng, 0, 1)
			failedAttempts = 0
		}
		credits = fourierScore(rng, t, creditsWave, 0, 140, 8)
		load = fourierScore(rng, t, loadWave, 10, 22, 1.2)
		prefMatch = fourierScore(rng, t, prefMatchWave, 1, 5, 0.4)
		altSecCount = fourierScore(rng, t, altSecWave, 0, 5, 0.5)
		canOpen = fourierScore(rng, t, canOpenWave, 1, 5, 0.4)

		c := rng.Intn(numCourses)
		secs := courseSections[c]
		primary := secs[rng.Intn(len(secs))]
		alt := ""
		if len(secs) > 1 {
			for {
				cand := secs[rng.Intn(len(secs))]
				if cand != primary {
					alt = cand
					break
				}
			}
		}

		students[i] = simStudent{
			ID: fmt.Sprintf("SV%06d", i), Urgent: urgent,
			Scores: []float64{
				float64(mandatory), float64(gradRisk), float64(semWaited), float64(failedAttempts),
				float64(credits), float64(load), 0, float64(prefMatch), float64(altSecCount), float64(canOpen),
			},
			CourseID: courseOfSection[primary], PrimarySec: primary, AltSec: alt,
			SubmittedIdx: order[i],
		}
	}
	return students, sections, courseOfSection
}

// ---------------------------------------------------------------------
// 4 CHIẾN LƯỢC PHÂN BỔ
// ---------------------------------------------------------------------

type simResult struct {
	Strategy         string
	NumStudents      int
	OverloadRatio    float64
	SuccessRate      float64
	UrgentSuccess    float64
	NormalSuccess    float64
	FairnessIndex    float64 // Jain's index trên 2 tỉ lệ thành công (urgent, normal)
	CapacityUtilPct  float64
	ProcessingTimeMs float64
}

func evaluate(strategy string, numStudents int, overloadRatio float64,
	confirmed map[string]bool, students []simStudent, sections map[string]Section, elapsed time.Duration) simResult {

	var urgentTotal, urgentOK, normalTotal, normalOK, totalOK int
	for _, s := range students {
		ok := confirmed[s.ID]
		if ok {
			totalOK++
		}
		if s.Urgent {
			urgentTotal++
			if ok {
				urgentOK++
			}
		} else {
			normalTotal++
			if ok {
				normalOK++
			}
		}
	}
	urgentRate := ratio(urgentOK, urgentTotal)
	normalRate := ratio(normalOK, normalTotal)
	fairness := jainFairness([]float64{urgentRate, normalRate})

	totalCap := 0
	for _, sec := range sections {
		totalCap += sec.Capacity
	}
	util := ratio(totalOK, totalCap)

	return simResult{
		Strategy: strategy, NumStudents: numStudents, OverloadRatio: overloadRatio,
		SuccessRate: ratio(totalOK, numStudents), UrgentSuccess: urgentRate, NormalSuccess: normalRate,
		FairnessIndex: fairness, CapacityUtilPct: util * 100, ProcessingTimeMs: float64(elapsed.Microseconds()) / 1000.0,
	}
}

func ratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func jainFairness(xs []float64) float64 {
	var sum, sumSq float64
	for _, x := range xs {
		sum += x
		sumSq += x * x
	}
	if sumSq == 0 {
		return 1
	}
	return (sum * sum) / (float64(len(xs)) * sumSq)
}

// runFIFO: sắp theo thứ tự gửi (SubmittedIdx tăng dần), lấp đầy đúng
// section ưu tiên duy nhất mà sinh viên khai báo -- KHÔNG xét luân chuyển.
func runFIFO(students []simStudent, sections map[string]Section) (map[string]bool, time.Duration) {
	start := time.Now()
	ordered := make([]simStudent, len(students))
	copy(ordered, students)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].SubmittedIdx < ordered[j].SubmittedIdx })

	capLeft := cloneCapacity(sections)
	confirmed := make(map[string]bool, len(students))
	for _, s := range ordered {
		if capLeft[s.PrimarySec] > 0 {
			capLeft[s.PrimarySec]--
			confirmed[s.ID] = true
		}
	}
	return confirmed, time.Since(start)
}

// runRuleBased: ưu tiên cố định theo luật cứng (không cần TOPSIS): mandatory
// giảm dần -> graduation_delay_risk giảm dần -> semesters_waited giảm dần.
// Đại diện đúng cách "Rule-based: ưu tiên theo luật cứng, ví dụ năm cuối
// trước" trong thiết kế thí nghiệm.
func runRuleBased(students []simStudent, sections map[string]Section) (map[string]bool, time.Duration) {
	start := time.Now()
	ordered := make([]simStudent, len(students))
	copy(ordered, students)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.Scores[0] != b.Scores[0] {
			return a.Scores[0] > b.Scores[0] // mandatory
		}
		if a.Scores[1] != b.Scores[1] {
			return a.Scores[1] > b.Scores[1] // graduation_delay_risk
		}
		return a.Scores[2] > b.Scores[2] // semesters_waited
	})

	capLeft := cloneCapacity(sections)
	confirmed := make(map[string]bool, len(students))
	for _, s := range ordered {
		if capLeft[s.PrimarySec] > 0 {
			capLeft[s.PrimarySec]--
			confirmed[s.ID] = true
		}
	}
	return confirmed, time.Since(start)
}

// runTopsisOnly: xếp hạng bằng TOPSIS-Business thật (EWM), nhưng phân bổ
// GREEDY THEO ĐÚNG SECTION ƯU TIÊN ĐÃ KHAI BÁO -- không luân chuyển sang
// lớp khác dù còn chỗ. Đại diện cho "TOPSIS: chỉ xếp hạng ưu tiên" trong
// thiết kế thí nghiệm (thiếu bước tối ưu phân bổ toàn cục).
func runTopsisOnly(students []simStudent, sections map[string]Section) (map[string]bool, time.Duration) {
	start := time.Now()

	requests := toRequests(students)
	engine := TOPSISBusiness{Criteria: criteria, Weights: WeightConfig{Method: EWM}}
	ranked, err := engine.RankByCourse(requests)
	if err != nil {
		panic(err)
	}

	capLeft := cloneCapacity(sections)
	confirmed := make(map[string]bool, len(students))
	for _, pool := range ranked {
		for _, rr := range pool {
			sec := rr.Request.SectionID // = PrimarySec (xem toRequests)
			if capLeft[sec] > 0 {
				capLeft[sec]--
				confirmed[rr.Request.StudentID] = true
			}
		}
	}
	return confirmed, time.Since(start)
}

// runHybrid: TOPSIS-Business xếp hạng + AllocationOptimizer phân bổ toàn
// cục, CÓ dùng cả nguyện vọng phụ (AltSec) khi có -- đại diện đúng cho
// "Hybrid: TOPSIS + giải bài toán phân bổ toàn cục" trong thiết kế.
func runHybrid(students []simStudent, sections map[string]Section) (map[string]bool, time.Duration) {
	start := time.Now()

	requests := toRequests(students)
	engine := TOPSISBusiness{Criteria: criteria, Weights: WeightConfig{Method: EWM}}
	ranked, err := engine.RankByCourse(requests)
	if err != nil {
		panic(err)
	}

	// scoreByStudent: điểm TOPSIS của mỗi sinh viên (không đổi theo lớp cụ
	// thể trong mô phỏng này -- cùng mức độ hài lòng với mọi lớp của cùng
	// môn học, chỉ khác về khả năng còn chỗ).
	scoreByStudent := make(map[string]float64, len(students))
	for _, pool := range ranked {
		for _, rr := range pool {
			scoreByStudent[rr.Request.StudentID] = rr.Score
		}
	}

	var edges []Edge
	byID := make(map[string]simStudent, len(students))
	for _, s := range students {
		byID[s.ID] = s
		score := scoreByStudent[s.ID]
		edges = append(edges, Edge{StudentID: s.ID, CourseID: s.CourseID, SectionID: s.PrimarySec, Score: score})
		if s.AltSec != "" {
			// Nguyện vọng phụ: cùng điểm ưu tiên (sinh viên coi 2 lớp của
			// cùng môn là như nhau), chỉ khác availability -- cho phép
			// AllocationOptimizer luân chuyển khi lớp ưu tiên hết chỗ.
			edges = append(edges, Edge{StudentID: s.ID, CourseID: s.CourseID, SectionID: s.AltSec, Score: score})
		}
	}

	optimizer := AllocationOptimizer{Sections: sections, MaxSwapIter: 30}
	result := optimizer.Solve(edges)

	confirmed := make(map[string]bool, len(students))
	for _, a := range result.Confirmed {
		confirmed[a.StudentID] = true
	}
	return confirmed, time.Since(start)
}

func toRequests(students []simStudent) []StudentRequest {
	requests := make([]StudentRequest, len(students))
	for i, s := range students {
		requests[i] = StudentRequest{
			StudentID: s.ID, CourseID: s.CourseID, SectionID: s.PrimarySec,
			Scores: s.Scores, SubmittedAt: time.Now(),
			PrerequisitesMet: true, CreditLoadOK: true, EligibleForProgram: true,
		}
	}
	return requests
}

func cloneCapacity(sections map[string]Section) map[string]int {
	out := make(map[string]int, len(sections))
	for id, s := range sections {
		out[id] = s.Capacity
	}
	return out
}

// ---------------------------------------------------------------------
// MAIN: chạy đúng ma trận thí nghiệm trong thiết kế
// ---------------------------------------------------------------------

func main() {
	os.MkdirAll("output", 0o755)

	// ---- Bảng 1: Phương pháp x Quy mô tải (overload cố định = 2.0) ----
	loadLevels := []int{1000, 5000, 10000, 20000}
	table1 := runMatrix(loadLevels, []float64{2.0})
	writeCSV("output/table1_by_load.csv", table1)
	printTable("BẢNG 1: Phương pháp × Quy mô tải (quá tải cố định = 2.0x sức chứa)", table1)

	// ---- Bảng 2: Phương pháp x Mức quá tải (tải cố định = 5000) ----
	overloadLevels := []float64{1.0, 1.5, 2.0, 5.0}
	table2 := runMatrix([]int{5000}, overloadLevels)
	writeCSV("output/table2_by_overload.csv", table2)
	printTable("BẢNG 2: Phương pháp × Mức quá tải (quy mô cố định = 5.000 sinh viên)", table2)
}

func runMatrix(loadLevels []int, overloadLevels []float64) []simResult {
	strategies := []struct {
		Name string
		Fn   func([]simStudent, map[string]Section) (map[string]bool, time.Duration)
	}{
		{"FIFO", runFIFO},
		{"Rule-based", runRuleBased},
		{"TOPSIS", runTopsisOnly},
		{"Hybrid", runHybrid},
	}

	var results []simResult
	seed := int64(42)
	for _, n := range loadLevels {
		for _, ov := range overloadLevels {
			students, sections, _ := generateData(seed, n, ov)
			for _, strat := range strategies {
				confirmed, elapsed := strat.Fn(students, sections)
				results = append(results, evaluate(strat.Name, n, ov, confirmed, students, sections, elapsed))
			}
		}
	}
	return results
}

// ---------------------------------------------------------------------
// IN BẢNG + XUẤT CSV
// ---------------------------------------------------------------------

func printTable(title string, results []simResult) {
	fmt.Println("\n" + title)
	fmt.Println(repeat("=", len(title)))
	fmt.Printf("%-10s %8s %8s %10s %10s %10s %10s %10s %12s\n",
		"Method", "N", "Overload", "Success%", "Urgent%", "Normal%", "Fairness", "Cap.Util%", "Time(ms)")
	for _, r := range results {
		fmt.Printf("%-10s %8d %8.1f %10.2f %10.2f %10.2f %10.4f %10.2f %12.3f\n",
			r.Strategy, r.NumStudents, r.OverloadRatio, r.SuccessRate*100, r.UrgentSuccess*100,
			r.NormalSuccess*100, r.FairnessIndex, r.CapacityUtilPct, r.ProcessingTimeMs)
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s[0])
	}
	return string(out)
}

func writeCSV(path string, results []simResult) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Println("lỗi ghi CSV:", err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	_ = w.Write([]string{"method", "num_students", "overload_ratio", "success_rate_pct",
		"urgent_success_pct", "normal_success_pct", "fairness_index", "capacity_util_pct", "processing_time_ms"})
	for _, r := range results {
		_ = w.Write([]string{
			r.Strategy, strconv.Itoa(r.NumStudents), strconv.FormatFloat(r.OverloadRatio, 'f', 2, 64),
			strconv.FormatFloat(r.SuccessRate*100, 'f', 4, 64),
			strconv.FormatFloat(r.UrgentSuccess*100, 'f', 4, 64),
			strconv.FormatFloat(r.NormalSuccess*100, 'f', 4, 64),
			strconv.FormatFloat(r.FairnessIndex, 'f', 6, 64),
			strconv.FormatFloat(r.CapacityUtilPct, 'f', 4, 64),
			strconv.FormatFloat(r.ProcessingTimeMs, 'f', 3, 64),
		})
	}
}