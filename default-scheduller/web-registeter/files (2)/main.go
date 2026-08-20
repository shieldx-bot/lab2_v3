// Chương trình mô phỏng thí nghiệm so sánh 4 chiến lược phân bổ đăng ký
// học phần: (1) FIFO, (2) Rule-based cố định, (3) TOPSIS đơn lẻ, (4) Hybrid
// (TOPSIS + AllocationOptimizer toàn cục).
//
// Dùng ĐÚNG code nghiệp vụ thật (topsis_core.go): RuleEngine.Filter,
// TOPSISBusiness.RankByCourse (cả EWM lẫn AHP), AllocationOptimizer.Solve
// (có ma trận xung đột lịch thật) -- không viết lại thuật toán riêng.
//
// SỬA SO VỚI BẢN TRƯỚC:
//   1. RuleEngine.Filter được gọi thật cho M2/M3/M4 (trước đây gán cứng
//      mọi ràng buộc = true, Rule Engine không hề chạy).
//   2. Có cả 2 phương pháp trọng số EWM và AHP, chạy song song và so sánh.
//   3. Lặp lại NumSeeds lần mỗi (kịch bản × chiến lược × phương pháp trọng
//      số) và xuất mean ± std, thay vì 1 lần chạy duy nhất với seed cố định.
//   4. Đo thời gian tách riêng 3 giai đoạn: Filter / Rank / Allocate, thay
//      vì đo gộp toàn hàm.
//   5. Có ma trận xung đột lịch (ScheduleConflicts) thật, sinh từ khung giờ
//      (TimeSlot) của từng lớp, dùng thật trong AllocationOptimizer -- KB5
//      "trùng lịch mạnh" giờ thực sự khác các kịch bản khác (giảm số
//      khung giờ khả dụng để tăng mật độ xung đột), không còn là TODO.
//   6. TOÀN BỘ tham số điều khiển thực nghiệm được gom vào 1 struct
//      ExperimentConfig() duy nhất ở đầu file, dễ đọc / dễ sửa.
//
// Chạy:  go run .
// Kết quả: in bảng tổng hợp ra console + ghi CSV vào ./output/
//   - output/raw_runs.csv         : từng lần chạy (1 dòng / seed)
//   - output/summary_mean_std.csv : tổng hợp mean ± std theo (kịch bản, chiến lược, phương pháp trọng số)
//   - output/arrival_histograms.csv
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

// ============================================================================
// 0. CẤU HÌNH THỰC NGHIỆM -- TẤT CẢ THAM SỐ Ở MỘT NƠI DUY NHẤT
// ============================================================================
//
// Đổi bất kỳ giá trị nào ở đây là đủ để chạy lại toàn bộ ma trận thực
// nghiệm với cấu hình khác, không cần sửa logic ở phía dưới.

type ExperimentConfig struct {
	// --- Cấu trúc học phần ---
	NumCourses        int // số học phần mô phỏng
	SectionsPerCourse int // số lớp mỗi học phần

	// --- Độ tin cậy thống kê ---
	NumSeeds int   // số lần lặp độc lập cho MỖI (kịch bản × chiến lược × phương pháp trọng số)
	BaseSeed int64 // seed gốc; seed lần lặp thứ k = BaseSeed + int64(k)

	// --- Phương pháp trọng số TOPSIS được chạy và so sánh ---
	WeightMethods []WeightMethod // {EWM, AHP}

	// --- Bộ tối ưu phân bổ toàn cục ---
	MaxSwapIter int // số vòng lặp local-search tối đa trong AllocationOptimizer.Solve

	// --- Tỉ lệ vi phạm ràng buộc cứng, dùng bởi RuleEngine.Filter thật ---
	// 2 mức theo đúng thiết kế: "low" ~5%, "high" ~25%. Mỗi sinh viên bị
	// vi phạm ĐÚNG 1 trong 5 lý do (loại trừ lẫn nhau), chọn theo phân phối
	// có trọng số (xem hardConstraintReasonWeights).
	ViolationRates map[string]float64

	// --- Khung giờ dùng để sinh xung đột lịch giữa các lớp khác môn ---
	NumTimeSlots int // mặc định; từng kịch bản có thể override (ít khung giờ hơn -> nhiều xung đột hơn)
}

var expCfg = ExperimentConfig{
	NumCourses:        50,
	SectionsPerCourse: 2.4,
	NumSeeds:          30,
	BaseSeed:          42,
	WeightMethods:     []WeightMethod{EWM, AHP},
	MaxSwapIter:        30,
	ViolationRates: map[string]float64{
		"low":  0.05,
		"high": 0.25,
	},
	NumTimeSlots: 12,
}

// 10 tiêu chí TOPSIS, đúng thứ tự dùng xuyên suốt mô phỏng.
var criteria = []Criterion{
	{Name: "mandatory", Type: Benefit},
	{Name: "graduation_delay_risk", Type: Benefit},
	{Name: "semesters_waited", Type: Benefit},
	{Name: "failed_attempts", Type: Benefit},
	{Name: "credits_completed", Type: Benefit},
	{Name: "current_semester_load", Type: Cost},
	{Name: "schedule_conflict", Type: Cost}, // luôn = 0 (chưa dùng làm điểm số; xung đột thật xử lý riêng qua HasScheduleClash/Conflicts)
	{Name: "preference_match", Type: Benefit},
	{Name: "alternative_sections", Type: Cost},
	{Name: "can_open_more_sections", Type: Cost},
}

// ahpTargetWeights: trọng số "chuyên gia" mục tiêu cho AHP, theo đúng thứ
// tự criteria ở trên, tổng = 1. Ma trận so sánh cặp AHP được DỰNG NGƯỢC từ
// vector này (pcm[i][j] = w[i]/w[j]) nên luôn hoàn toàn nhất quán (CR = 0)
// -- đảm bảo ahpWeights() trong topsis_core.go không bao giờ trả lỗi
// "inconsistent" trong mô phỏng. Đây là lựa chọn có chủ đích để tách biệt
// việc kiểm định ĐỘ NHẤT QUÁN AHP (đã có unit test riêng ở mục 7 kịch bản
// biên của thiết kế) khỏi việc so sánh HIỆU QUẢ EWM vs AHP ở thực nghiệm
// chính này.
var ahpTargetWeights = []float64{
	0.15, // mandatory
	0.18, // graduation_delay_risk
	0.13, // semesters_waited
	0.08, // failed_attempts
	0.06, // credits_completed
	0.05, // current_semester_load
	0.10, // schedule_conflict
	0.10, // preference_match
	0.12, // alternative_sections
	0.03, // can_open_more_sectionss
} 

func buildAHPMatrixFromWeights(w []float64) [][]float64 {
	n := len(w)
	m := make([][]float64, n)
	for i := 0; i < n; i++ {
		m[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			m[i][j] = w[i] / w[j]
		}
	}
	return m
}

// hardConstraintReasonWeights: trọng số chọn "lý do vi phạm" khi 1 sinh
// viên bị đánh dấu vi phạm ràng buộc cứng. Mặc định đều nhau; kịch bản
// KB5 (trùng lịch mạnh) tăng mạnh trọng số "clash" để mô phỏng đúng chủ đề
// của kịch bản đó.
type reasonWeights struct {
	Prereq, Credit, Clash, Duplicate, Eligibility float64
}

func defaultReasonWeights() reasonWeights {
	return reasonWeights{Prereq: 1, Credit: 1, Clash: 1, Duplicate: 1, Eligibility: 1}
}

func randInt(rng *rand.Rand, min, max int) int { return min + rng.Intn(max-min+1) }

// ---------------------------------------------------------------------
// MÔ HÌNH THỜI ĐIỂM ĐẾN: CHUỖI FOURIER
// ---------------------------------------------------------------------
//
// Lý do dùng Fourier thay vì hoán vị đều: hoán vị đều giả định xác suất
// đến là như nhau tại mọi thời điểm trong cửa sổ đăng ký -- không phản
// ánh hiện tượng "thundering herd" (SV đồng loạt bấm nút đúng giờ mở
// cổng). Mô hình cường độ đến λ(t) dưới đây là 1 chuỗi Fourier hữu hạn,
// cho phép tạo nhiều hình dạng đỉnh khác nhau (phẳng / đỉnh vừa / đỉnh
// nhọn) chỉ bằng cách đổi hệ số hài -- ánh xạ trực tiếp theo từng kịch
// bản. Việc lặp lại NumSeeds lần với cùng 1 ArrivalProfile (chỉ đổi seed
// rng) giúp ước lượng được phương sai do nhiễu lấy mẫu của chính mô hình
// Fourier này, tăng độ tin cậy so với 1 lần chạy duy nhất.

type Harmonic struct {
	Amplitude float64
	Order     int
	Phase     float64
}

type ArrivalProfile struct {
	Base      float64
	Harmonics []Harmonic
}

func (p ArrivalProfile) intensity(t float64) float64 {
	v := p.Base
	for _, h := range p.Harmonics {
		v += h.Amplitude * math.Cos(2*math.Pi*float64(h.Order)*(t-h.Phase))
	}
	if v < p.Base*0.001 {
		v = p.Base * 0.001
	}
	return v
}

func sampleArrivalTimes(rng *rand.Rand, n int, profile ArrivalProfile) []float64 {
	lambdaMax := 0.0
	for i := 0; i < 1000; i++ {
		t := float64(i) / 1000.0
		if v := profile.intensity(t); v > lambdaMax {
			lambdaMax = v
		}
	}
	times := make([]float64, n)
	for i := 0; i < n; i++ {
		for {
			t := rng.Float64()
			if rng.Float64()*lambdaMax <= profile.intensity(t) {
				times[i] = t
				break
			}
		}
	}
	return times
}

func arrivalOrderFromTimes(times []float64) []int {
	type idxTime struct {
		Idx  int
		Time float64
	}
	pairs := make([]idxTime, len(times))
	for i, t := range times {
		pairs[i] = idxTime{i, t}
	}
	sort.SliceStable(pairs, func(a, b int) bool { return pairs[a].Time < pairs[b].Time })
	order := make([]int, len(times))
	for rank, p := range pairs {
		order[p.Idx] = rank
	}
	return order
}

// ---------------------------------------------------------------------
// KỊCH BẢN
// ---------------------------------------------------------------------

type ScenarioConfig struct {
	Name          string
	NumStudents   int
	OverloadRatio float64
	UrgentRatio   float64
	Arrival       ArrivalProfile

	ViolationLevel string        // "low" hoặc "high" -- tra vào expCfg.ViolationRates
	ReasonWeights  reasonWeights // trọng số chọn lý do vi phạm cụ thể
	NumTimeSlots   int           // 0 = dùng expCfg.NumTimeSlots; KB5 override thấp hơn để tăng mật độ xung đột
}

var scenarios = []ScenarioConfig{
	{
		Name: "KB1_NhuCauThap", NumStudents: 3000, OverloadRatio: 0.6, UrgentRatio: 0.15,
		Arrival:        ArrivalProfile{Base: 1.0, Harmonics: []Harmonic{{Amplitude: 0.1, Order: 1, Phase: 0}}},
		ViolationLevel: "low", ReasonWeights: defaultReasonWeights(),
	},
	{
		Name: "KB2_NhuCauBangChiTieu", NumStudents: 3000, OverloadRatio: 1.0, UrgentRatio: 0.15,
		Arrival:        ArrivalProfile{Base: 1.0, Harmonics: []Harmonic{{Amplitude: 0.3, Order: 1, Phase: 0.05}}},
		ViolationLevel: "low", ReasonWeights: defaultReasonWeights(),
	},
	{
		Name: "KB3_QuaTaiVua", NumStudents: 5000, OverloadRatio: 1.75, UrgentRatio: 0.15,
		Arrival: ArrivalProfile{Base: 1.0, Harmonics: []Harmonic{
			{Amplitude: 0.8, Order: 1, Phase: 0.05},
			{Amplitude: 0.3, Order: 3, Phase: 0.05},
		}},
		ViolationLevel: "low", ReasonWeights: defaultReasonWeights(),
	},
	{
		Name: "KB4_QuaTaiNang", NumStudents: 1000, OverloadRatio: 4.0, UrgentRatio: 0.15,
		Arrival: ArrivalProfile{Base: 1.0, Harmonics: []Harmonic{
			{Amplitude: 1.5, Order: 1, Phase: 0.02},
			{Amplitude: 1.0, Order: 4, Phase: 0.02},
			{Amplitude: 0.5, Order: 8, Phase: 0.02},
		}},
		ViolationLevel: "high", ReasonWeights: defaultReasonWeights(), // tải cực lớn thường đi kèm nhiều lỗi đăng ký hơn (SV thao tác vội, hệ thống nghẽn)
	},
	{
		// KB5 trùng lịch mạnh: NAY THỰC SỰ khác biệt -- giảm NumTimeSlots
		// (ít khung giờ khả dụng hơn -> mật độ trùng lịch giữa các lớp
		// khác môn tăng mạnh, dùng thật trong AllocationOptimizer.Conflicts)
		// và tăng mạnh trọng số lý do "clash" khi RuleEngine từ chối.
		Name: "KB5_TrungLichManh", NumStudents: 5000, OverloadRatio: 1.5, UrgentRatio: 0.15,
		Arrival:        ArrivalProfile{Base: 1.0, Harmonics: []Harmonic{{Amplitude: 0.3, Order: 1, Phase: 0.05}}},
		ViolationLevel: "low",
		ReasonWeights:  reasonWeights{Prereq: 1, Credit: 1, Clash: 6, Duplicate: 1, Eligibility: 1},
		NumTimeSlots:   3, // ít khung giờ hơn hẳn mặc định (12) -> xung đột dày đặc
	},
	{
		Name: "KB6_NhieuSVNamCuoi", NumStudents: 5000, OverloadRatio: 1.5, UrgentRatio: 0.45,
		Arrival:        ArrivalProfile{Base: 1.0, Harmonics: []Harmonic{{Amplitude: 0.3, Order: 1, Phase: 0.05}}},
		ViolationLevel: "low", ReasonWeights: defaultReasonWeights(),
	},
}

// ---------------------------------------------------------------------
// MÔ HÌNH DỮ LIỆU MÔ PHỎNG
// ---------------------------------------------------------------------

type simStudent struct {
	ID           string
	Urgent       bool
	Scores       []float64
	CourseID     string
	PrimarySec   string
	AltSec       string
	SubmittedIdx int
	ArrivalTime  float64

	// Cờ ràng buộc cứng -- SINH THẬT theo ViolationRate/ReasonWeights của
	// kịch bản, và được RuleEngine.Filter thật sự dùng để lọc (khác bản
	// trước: mọi cờ trước đây bị gán cứng = true/false, không mô phỏng gì).
	PrerequisitesMet   bool
	CreditLoadOK       bool
	HasScheduleClash   bool
	AlreadyRegistered  bool
	EligibleForProgram bool
}

// generateData sinh sinh viên + lớp + ma trận xung đột lịch cho 1
// (kịch bản, seed) cụ thể.
func generateData(seed int64, cfg ScenarioConfig) ([]simStudent, map[string]Section, ScheduleConflicts) {
	rng := rand.New(rand.NewSource(seed))
	numStudents := cfg.NumStudents

	totalSections := expCfg.NumCourses * expCfg.SectionsPerCourse
	totalCapacity := int(float64(numStudents) / cfg.OverloadRatio)
	if totalCapacity < totalSections {
		totalCapacity = totalSections
	}
	capPerSection := totalCapacity / totalSections

	numSlots := cfg.NumTimeSlots
	if numSlots <= 0 {
		numSlots = expCfg.NumTimeSlots
	}

	sections := make(map[string]Section, totalSections)
	courseOfSection := make(map[string]string, totalSections)
	sectionTimeSlot := make(map[string]int, totalSections)
	courseSections := make([][]string, expCfg.NumCourses)
	for c := 0; c < expCfg.NumCourses; c++ {
		courseID := fmt.Sprintf("MH%03d", c)
		for s := 0; s < expCfg.SectionsPerCourse; s++ {
			secID := fmt.Sprintf("%s-L%d", courseID, s)
			sections[secID] = Section{ID: secID, CourseID: courseID, Capacity: capPerSection}
			courseOfSection[secID] = courseID
			courseSections[c] = append(courseSections[c], secID)
			sectionTimeSlot[secID] = rng.Intn(numSlots)
		}
	}

	// Ma trận xung đột lịch THẬT: 2 lớp của 2 học phần KHÁC NHAU cùng
	// khung giờ thì xung đột. Lớp cùng học phần không đánh dấu xung đột
	// (sinh viên chỉ chọn 1 trong số đó, đã được AllocationOptimizer xử
	// lý qua assignedCourse, không cần Conflicts).
	conflicts := make(ScheduleConflicts)
	secIDs := make([]string, 0, totalSections)
	for id := range sections {
		secIDs = append(secIDs, id)
	}
	for i := 0; i < len(secIDs); i++ {
		for j := i + 1; j < len(secIDs); j++ {
			a, b := secIDs[i], secIDs[j]
			if courseOfSection[a] == courseOfSection[b] {
				continue
			}
			if sectionTimeSlot[a] == sectionTimeSlot[b] {
				if conflicts[a] == nil {
					conflicts[a] = make(map[string]bool)
				}
				if conflicts[b] == nil {
					conflicts[b] = make(map[string]bool)
				}
				conflicts[a][b] = true
				conflicts[b][a] = true
			}
		}
	}

	arrivalTimes := sampleArrivalTimes(rng, numStudents, cfg.Arrival)
	order := arrivalOrderFromTimes(arrivalTimes)

	violationRate := expCfg.ViolationRates[cfg.ViolationLevel]
	rw := cfg.ReasonWeights
	totalW := rw.Prereq + rw.Credit + rw.Clash + rw.Duplicate + rw.Eligibility

	students := make([]simStudent, numStudents)
	for i := 0; i < numStudents; i++ {
		urgent := rng.Float64() < cfg.UrgentRatio
		var mandatory, gradRisk, semWaited, failedAttempts, credits, load, prefMatch, altSecCount, canOpen int
		mandatory = randInt(rng, 1, 5)
		if urgent {
			gradRisk = randInt(rng, 4, 5)
			semWaited = randInt(rng, 2, 5)
			failedAttempts = randInt(rng, 1, 3)
		} else {
			gradRisk = randInt(rng, 1, 3)
			semWaited = randInt(rng, 0, 1)
			failedAttempts = 0
		}
		credits = randInt(rng, 0, 140)
		load = randInt(rng, 10, 22)
		prefMatch = randInt(rng, 1, 5)
		altSecCount = randInt(rng, 0, 5)
		canOpen = randInt(rng, 1, 5)

		c := rng.Intn(expCfg.NumCourses)
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

		// --- Sinh cờ ràng buộc cứng THẬT ---
		prereqOK, creditOK, clash, dup, elig := true, true, false, false, true
		if rng.Float64() < violationRate {
			pick := rng.Float64() * totalW
			switch {
			case pick < rw.Prereq:
				prereqOK = false
			case pick < rw.Prereq+rw.Credit:
				creditOK = false
			case pick < rw.Prereq+rw.Credit+rw.Clash:
				clash = true
			case pick < rw.Prereq+rw.Credit+rw.Clash+rw.Duplicate:
				dup = true
			default:
				elig = false
			}
		}

		students[i] = simStudent{
			ID: fmt.Sprintf("SV%06d", i), Urgent: urgent,
			Scores: []float64{
				float64(mandatory), float64(gradRisk), float64(semWaited), float64(failedAttempts),
				float64(credits), float64(load), 0, float64(prefMatch), float64(altSecCount), float64(canOpen),
			},
			CourseID: courseOfSection[primary], PrimarySec: primary, AltSec: alt,
			SubmittedIdx: order[i], ArrivalTime: arrivalTimes[i],
			PrerequisitesMet: prereqOK, CreditLoadOK: creditOK, HasScheduleClash: clash,
			AlreadyRegistered: dup, EligibleForProgram: elig,
		}
	}
	return students, sections, conflicts
}

// ---------------------------------------------------------------------
// KẾT QUẢ ĐO LƯỜNG
// ---------------------------------------------------------------------

type simResult struct {
	Scenario       string
	Strategy       string
	WeightMethod   string // "-" cho FIFO/Rule-based (không dùng TOPSIS); "EWM" hoặc "AHP" cho TOPSIS/Hybrid
	Seed           int64
	NumStudents    int
	OverloadRatio  float64
	ViolationLevel string

	RejectedHard int // số bị RuleEngine loại (ràng buộc cứng)
	WaitlistN    int // số vào danh sách chờ (chỉ Hybrid có ý nghĩa đầy đủ; các chiến lược khác = số bị từ chối do hết chỗ)

	SuccessRate     float64
	UrgentSuccess   float64
	NormalSuccess   float64
	FairnessIndex   float64
	CapacityUtilPct float64

	FilterMs   float64
	RankMs     float64
	AllocateMs float64
	TotalMs    float64
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

func evaluate(strategy, weightMethod string, cfg ScenarioConfig, seed int64,
	confirmed map[string]bool, students []simStudent, sections map[string]Section,
	rejectedHard, waitlistN int, filterMs, rankMs, allocMs float64) simResult {

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
		Scenario: cfg.Name, Strategy: strategy, WeightMethod: weightMethod, Seed: seed,
		NumStudents: cfg.NumStudents, OverloadRatio: cfg.OverloadRatio, ViolationLevel: cfg.ViolationLevel,
		RejectedHard: rejectedHard, WaitlistN: waitlistN,
		SuccessRate: ratio(totalOK, len(students)), UrgentSuccess: urgentRate, NormalSuccess: normalRate,
		FairnessIndex: fairness, CapacityUtilPct: util * 100,
		FilterMs: filterMs, RankMs: rankMs, AllocateMs: allocMs, TotalMs: filterMs + rankMs + allocMs,
	}
}

func cloneCapacity(sections map[string]Section) map[string]int {
	out := make(map[string]int, len(sections))
	for id, s := range sections {
		out[id] = s.Capacity
	}
	return out
}

func toRequests(students []simStudent, windowStart time.Time) []StudentRequest {
	requests := make([]StudentRequest, len(students))
	for i, s := range students {
		requests[i] = StudentRequest{
			StudentID: s.ID, CourseID: s.CourseID, SectionID: s.PrimarySec,
			Scores:      s.Scores,
			SubmittedAt: windowStart.Add(time.Duration(s.ArrivalTime * float64(4*time.Hour))), // cửa sổ đăng ký giả định 4 giờ
			PrerequisitesMet:   s.PrerequisitesMet,
			CreditLoadOK:       s.CreditLoadOK,
			HasScheduleClash:   s.HasScheduleClash,
			AlreadyRegistered:  s.AlreadyRegistered,
			EligibleForProgram: s.EligibleForProgram,
		}
	}
	return requests
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000.0 }

// ---------------------------------------------------------------------
// 4 CHIẾN LƯỢC PHÂN BỔ
// ---------------------------------------------------------------------

// runFIFO: KHÔNG dùng RuleEngine, KHÔNG dùng TOPSIS -- đại diện đúng cho
// hệ thống "ai đến trước phục vụ trước" thuần túy, làm baseline dưới cùng.
func runFIFO(students []simStudent, sections map[string]Section) (map[string]bool, int, int, float64, float64, float64) {
	tRank := time.Now()
	ordered := make([]simStudent, len(students))
	copy(ordered, students)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].SubmittedIdx < ordered[j].SubmittedIdx })
	rankMs := msSince(tRank)

	tAlloc := time.Now()
	capLeft := cloneCapacity(sections)
	confirmed := make(map[string]bool, len(students))
	waitlist := 0
	for _, s := range ordered {
		if capLeft[s.PrimarySec] > 0 {
			capLeft[s.PrimarySec]--
			confirmed[s.ID] = true
		} else {
			waitlist++
		}
	}
	allocMs := msSince(tAlloc)
	return confirmed, 0, waitlist, 0, rankMs, allocMs
}

// runRuleBased: NAY dùng RuleEngine.Filter THẬT để loại yêu cầu vi phạm
// ràng buộc cứng, sau đó xếp theo luật cố định (mandatory -> graduation
// risk -> semesters waited), rồi phân bổ greedy theo lớp ưu tiên duy nhất.
func runRuleBased(students []simStudent, sections map[string]Section, windowStart time.Time) (map[string]bool, int, int, float64, float64, float64) {
	requests := toRequests(students, windowStart)

	tFilter := time.Now()
	filtered := RuleEngine{}.Filter(requests)
	filterMs := msSince(tFilter)

	tRank := time.Now()
	eligible := make([]StudentRequest, len(filtered.Eligible))
	copy(eligible, filtered.Eligible)
	sort.SliceStable(eligible, func(i, j int) bool {
		a, b := eligible[i].Scores, eligible[j].Scores
		if a[0] != b[0] {
			return a[0] > b[0] // mandatory
		}
		if a[1] != b[1] {
			return a[1] > b[1] // graduation_delay_risk
		}
		return a[2] > b[2] // semesters_waited
	})
	rankMs := msSince(tRank)

	tAlloc := time.Now()
	capLeft := cloneCapacity(sections)
	confirmed := make(map[string]bool, len(students))
	waitlist := 0
	for _, r := range eligible {
		if capLeft[r.SectionID] > 0 {
			capLeft[r.SectionID]--
			confirmed[r.StudentID] = true
		} else {
			waitlist++
		}
	}
	allocMs := msSince(tAlloc)
	return confirmed, len(filtered.Rejected), waitlist, filterMs, rankMs, allocMs
}

// runTopsisOnly: RuleEngine.Filter THẬT + TOPSISBusiness.RankByCourse
// (EWM hoặc AHP tùy tham số weightMethod), phân bổ GREEDY THEO ĐÚNG
// SECTION ƯU TIÊN ĐÃ KHAI BÁO -- không luân chuyển sang lớp khác dù còn
// chỗ (thiếu bước tối ưu phân bổ toàn cục, đúng như thiết kế M3).
func runTopsisOnly(students []simStudent, sections map[string]Section, windowStart time.Time, weightMethod WeightMethod) (map[string]bool, int, int, float64, float64, float64) {
	requests := toRequests(students, windowStart)

	tFilter := time.Now()
	filtered := RuleEngine{}.Filter(requests)
	filterMs := msSince(tFilter)

	tRank := time.Now()
	wc := WeightConfig{Method: weightMethod}
	if weightMethod == AHP {
		wc.AHPPairwiseMatrix = buildAHPMatrixFromWeights(ahpTargetWeights)
	}
	engine := TOPSISBusiness{Criteria: criteria, Weights: wc}
	ranked, err := engine.RankByCourse(filtered.Eligible)
	if err != nil {
		panic(err)
	}
	rankMs := msSince(tRank)

	tAlloc := time.Now()
	capLeft := cloneCapacity(sections)
	confirmed := make(map[string]bool, len(students))
	waitlist := 0
	for _, pool := range ranked {
		for _, rr := range pool {
			sec := rr.Request.SectionID
			if capLeft[sec] > 0 {
				capLeft[sec]--
				confirmed[rr.Request.StudentID] = true
			} else {
				waitlist++
			}
		}
	}
	allocMs := msSince(tAlloc)
	return confirmed, len(filtered.Rejected), waitlist, filterMs, rankMs, allocMs
}

// runHybrid: RuleEngine.Filter THẬT + TOPSISBusiness.RankByCourse (EWM
// hoặc AHP) + AllocationOptimizer.Solve THẬT (dùng cả nguyện vọng phụ
// AltSec và ma trận xung đột lịch thật sinh từ TimeSlot).
func runHybrid(students []simStudent, sections map[string]Section, conflicts ScheduleConflicts,
	windowStart time.Time, weightMethod WeightMethod) (map[string]bool, int, int, float64, float64, float64) {

	requests := toRequests(students, windowStart)

	tFilter := time.Now()
	filtered := RuleEngine{}.Filter(requests)
	filterMs := msSince(tFilter)

	tRank := time.Now()
	wc := WeightConfig{Method: weightMethod}
	if weightMethod == AHP {
		wc.AHPPairwiseMatrix = buildAHPMatrixFromWeights(ahpTargetWeights)
	}
	engine := TOPSISBusiness{Criteria: criteria, Weights: wc}
	ranked, err := engine.RankByCourse(filtered.Eligible)
	if err != nil {
		panic(err)
	}
	scoreByStudent := make(map[string]float64, len(students))
	for _, pool := range ranked {
		for _, rr := range pool {
			scoreByStudent[rr.Request.StudentID] = rr.Score
		}
	}
	rankMs := msSince(tRank)

	tAlloc := time.Now()
	eligibleIDs := make(map[string]bool, len(filtered.Eligible))
	for _, r := range filtered.Eligible {
		eligibleIDs[r.StudentID] = true
	}

	var edges []Edge
	for _, s := range students {
		if !eligibleIDs[s.ID] {
			continue // bị RuleEngine loại, không đưa vào bài toán phân bổ
		}
		score := scoreByStudent[s.ID]
		edges = append(edges, Edge{StudentID: s.ID, CourseID: s.CourseID, SectionID: s.PrimarySec, Score: score})
		if s.AltSec != "" {
			edges = append(edges, Edge{StudentID: s.ID, CourseID: s.CourseID, SectionID: s.AltSec, Score: score})
		}
	}

	optimizer := AllocationOptimizer{Sections: sections, Conflicts: conflicts, MaxSwapIter: expCfg.MaxSwapIter}
	result := optimizer.Solve(edges)
	allocMs := msSince(tAlloc)

	confirmed := make(map[string]bool, len(students))
	for _, a := range result.Confirmed {
		confirmed[a.StudentID] = true
	}
	return confirmed, len(filtered.Rejected), len(result.Waitlist), filterMs, rankMs, allocMs
}

// ---------------------------------------------------------------------
// VÒNG LẶP THỰC NGHIỆM CHÍNH: LẶP QUA SEED ĐỂ TÍNH MEAN ± STD
// ---------------------------------------------------------------------

func runAllSeeds() []simResult {
	var raw []simResult
	windowStart := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)

	for _, cfg := range scenarios {
		for k := 0; k < expCfg.NumSeeds; k++ {
			seed := expCfg.BaseSeed + int64(k)
			students, sections, conflicts := generateData(seed, cfg)

			// M1: FIFO (không TOPSIS -> weightMethod "-")
			{
				confirmed, rej, wl, filterMs, rankMs, allocMs := runFIFO(students, sections)
				raw = append(raw, evaluate("FIFO", "-", cfg, seed, confirmed, students, sections, rej, wl, filterMs, rankMs, allocMs))
			}
			// M2: Rule-based (không TOPSIS -> weightMethod "-")
			{
				confirmed, rej, wl, filterMs, rankMs, allocMs := runRuleBased(students, sections, windowStart)
				raw = append(raw, evaluate("Rule-based", "-", cfg, seed, confirmed, students, sections, rej, wl, filterMs, rankMs, allocMs))
			}
			// M3 & M4: chạy cho từng phương pháp trọng số cấu hình sẵn
			for _, wm := range expCfg.WeightMethods {
				wmName := "EWM"
				if wm == AHP {
					wmName = "AHP"
				}
				{
					confirmed, rej, wl, filterMs, rankMs, allocMs := runTopsisOnly(students, sections, windowStart, wm)
					raw = append(raw, evaluate("TOPSIS", wmName, cfg, seed, confirmed, students, sections, rej, wl, filterMs, rankMs, allocMs))
				}
				{
					confirmed, rej, wl, filterMs, rankMs, allocMs := runHybrid(students, sections, conflicts, windowStart, wm)
					raw = append(raw, evaluate("Hybrid", wmName, cfg, seed, confirmed, students, sections, rej, wl, filterMs, rankMs, allocMs))
				}
			}
		}
	}
	return raw
}

// ---------------------------------------------------------------------
// TỔNG HỢP MEAN ± STD THEO (KỊCH BẢN, CHIẾN LƯỢC, PHƯƠNG PHÁP TRỌNG SỐ)
// ---------------------------------------------------------------------

type aggKey struct {
	Scenario, Strategy, WeightMethod string
}

type aggResult struct {
	aggKey
	N                                     int
	SuccessMean, SuccessStd               float64
	UrgentMean, UrgentStd                 float64
	NormalMean, NormalStd                 float64
	FairnessMean, FairnessStd             float64
	UtilMean, UtilStd                     float64
	FilterMsMean, RankMsMean, AllocMsMean float64
	TotalMsMean, TotalMsStd               float64
	RejectedHardMean, WaitlistMean        float64
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func stdev(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	m := mean(xs)
	var s float64
	for _, x := range xs {
		s += (x - m) * (x - m)
	}
	return math.Sqrt(s / float64(len(xs)-1))
}

func aggregate(raw []simResult) []aggResult {
	groups := make(map[aggKey][]simResult)
	var order []aggKey
	for _, r := range raw {
		k := aggKey{r.Scenario, r.Strategy, r.WeightMethod}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}

	var out []aggResult
	for _, k := range order {
		rs := groups[k]
		success := make([]float64, len(rs))
		urgent := make([]float64, len(rs))
		normal := make([]float64, len(rs))
		fair := make([]float64, len(rs))
		util := make([]float64, len(rs))
		filterMs := make([]float64, len(rs))
		rankMs := make([]float64, len(rs))
		allocMs := make([]float64, len(rs))
		totalMs := make([]float64, len(rs))
		rejected := make([]float64, len(rs))
		waitlist := make([]float64, len(rs))
		for i, r := range rs {
			success[i] = r.SuccessRate
			urgent[i] = r.UrgentSuccess
			normal[i] = r.NormalSuccess
			fair[i] = r.FairnessIndex
			util[i] = r.CapacityUtilPct
			filterMs[i] = r.FilterMs
			rankMs[i] = r.RankMs
			allocMs[i] = r.AllocateMs
			totalMs[i] = r.TotalMs
			rejected[i] = float64(r.RejectedHard)
			waitlist[i] = float64(r.WaitlistN)
		}
		out = append(out, aggResult{
			aggKey: k, N: len(rs),
			SuccessMean: mean(success), SuccessStd: stdev(success),
			UrgentMean: mean(urgent), UrgentStd: stdev(urgent),
			NormalMean: mean(normal), NormalStd: stdev(normal),
			FairnessMean: mean(fair), FairnessStd: stdev(fair),
			UtilMean: mean(util), UtilStd: stdev(util),
			FilterMsMean: mean(filterMs), RankMsMean: mean(rankMs), AllocMsMean: mean(allocMs),
			TotalMsMean: mean(totalMs), TotalMsStd: stdev(totalMs),
			RejectedHardMean: mean(rejected), WaitlistMean: mean(waitlist),
		})
	}
	return out
}

// ---------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------

func main() {
	os.MkdirAll("output", 0o755)

	fmt.Printf("Chạy thực nghiệm: %d kịch bản x %d seed x (2 chiến lược không-TOPSIS + 2 chiến lược TOPSIS x %d phương pháp trọng số)\n",
		len(scenarios), expCfg.NumSeeds, len(expCfg.WeightMethods))

	raw := runAllSeeds()
	writeRawCSV("output/raw_runs.csv", raw)

	agg := aggregate(raw)
	writeAggCSV("output/summary_mean_std.csv", agg)
	printAggTable("KẾT QUẢ TỔNG HỢP (mean ± std qua "+strconv.Itoa(expCfg.NumSeeds)+" seed)", agg)

	writeArrivalHistogramCSV("output/arrival_histograms.csv", scenarios)

	fmt.Println("\nĐã ghi: output/raw_runs.csv, output/summary_mean_std.csv, output/arrival_histograms.csv")
}

// ---------------------------------------------------------------------
// IN BẢNG + XUẤT CSV
// ---------------------------------------------------------------------

func printAggTable(title string, results []aggResult) {
	fmt.Println("\n" + title)
	fmt.Println(repeat("=", len(title)))
	fmt.Printf("%-22s %-10s %-4s %6s %10s %10s %10s %10s %9s %10s\n",
		"Scenario", "Method", "W", "N", "Success%", "Fairness", "CapUtil%", "Total(ms)", "Reject", "Waitlist")
	for _, r := range results {
		fmt.Printf("%-22s %-10s %-4s %6d %6.2f±%.1f %10.4f %6.2f±%.1f %10.3f %9.1f %10.1f\n",
			r.Scenario, r.Strategy, r.WeightMethod, r.N,
			r.SuccessMean*100, r.SuccessStd*100, r.FairnessMean, r.UtilMean, r.UtilStd,
			r.TotalMsMean, r.RejectedHardMean, r.WaitlistMean)
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s[0])
	}
	return string(out)
}

func f(v float64) string { return strconv.FormatFloat(v, 'f', 6, 64) }

func writeRawCSV(path string, results []simResult) {
	fh, err := os.Create(path)
	if err != nil {
		fmt.Println("lỗi ghi CSV:", err)
		return
	}
	defer fh.Close()
	w := csv.NewWriter(fh)
	defer w.Flush()

	_ = w.Write([]string{
		"scenario", "strategy", "weight_method", "seed", "num_students", "overload_ratio", "violation_level",
		"rejected_hard", "waitlist", "success_rate_pct", "urgent_success_pct", "normal_success_pct",
		"fairness_index", "capacity_util_pct", "filter_ms", "rank_ms", "allocate_ms", "total_ms",
	})
	for _, r := range results {
		_ = w.Write([]string{
			r.Scenario, r.Strategy, r.WeightMethod, strconv.FormatInt(r.Seed, 10),
			strconv.Itoa(r.NumStudents), f(r.OverloadRatio), r.ViolationLevel,
			strconv.Itoa(r.RejectedHard), strconv.Itoa(r.WaitlistN),
			f(r.SuccessRate * 100), f(r.UrgentSuccess * 100), f(r.NormalSuccess * 100),
			f(r.FairnessIndex), f(r.CapacityUtilPct),
			f(r.FilterMs), f(r.RankMs), f(r.AllocateMs), f(r.TotalMs),
		})
	}
}

func writeAggCSV(path string, results []aggResult) {
	fh, err := os.Create(path)
	if err != nil {
		fmt.Println("lỗi ghi CSV:", err)
		return
	}
	defer fh.Close()
	w := csv.NewWriter(fh)
	defer w.Flush()

	_ = w.Write([]string{
		"scenario", "strategy", "weight_method", "n_seeds",
		"success_mean_pct", "success_std_pct", "urgent_mean_pct", "urgent_std_pct",
		"normal_mean_pct", "normal_std_pct", "fairness_mean", "fairness_std",
		"cap_util_mean_pct", "cap_util_std_pct", "filter_ms_mean", "rank_ms_mean", "allocate_ms_mean",
		"total_ms_mean", "total_ms_std", "rejected_hard_mean", "waitlist_mean",
	})
	for _, r := range results {
		_ = w.Write([]string{
			r.Scenario, r.Strategy, r.WeightMethod, strconv.Itoa(r.N),
			f(r.SuccessMean * 100), f(r.SuccessStd * 100), f(r.UrgentMean * 100), f(r.UrgentStd * 100),
			f(r.NormalMean * 100), f(r.NormalStd * 100), f(r.FairnessMean), f(r.FairnessStd),
			f(r.UtilMean), f(r.UtilStd), f(r.FilterMsMean), f(r.RankMsMean), f(r.AllocMsMean),
			f(r.TotalMsMean), f(r.TotalMsStd), f(r.RejectedHardMean), f(r.WaitlistMean),
		})
	}
}

func writeArrivalHistogramCSV(path string, list []ScenarioConfig) {
	const bins = 20
	fh, err := os.Create(path)
	if err != nil {
		fmt.Println("lỗi ghi CSV histogram:", err)
		return
	}
	defer fh.Close()
	w := csv.NewWriter(fh)
	defer w.Flush()
	_ = w.Write([]string{"scenario", "bin_index", "bin_start_t", "count"})

	for _, cfg := range list {
		rng := rand.New(rand.NewSource(expCfg.BaseSeed))
		times := sampleArrivalTimes(rng, cfg.NumStudents, cfg.Arrival)
		counts := make([]int, bins)
		for _, t := range times {
			b := int(t * float64(bins))
			if b >= bins {
				b = bins - 1
			}
			counts[b]++
		}
		for i, c := range counts {
			_ = w.Write([]string{cfg.Name, strconv.Itoa(i), strconv.FormatFloat(float64(i)/float64(bins), 'f', 3, 64), strconv.Itoa(c)})
		}
	}
}
