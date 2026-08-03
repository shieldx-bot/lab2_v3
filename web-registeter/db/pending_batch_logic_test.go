package main

import (
	"fmt"
	"sort"
	"testing"
	"time"

)

// fakeRow mô phỏng 1 dòng trong bảng `dang_ky` thật (chỉ giữ cột liên quan).
type fakeRow struct {
	MaDangKy     string
	MaSinhVien   string
	MaLopHocPhan string
	MaMonHoc     string
	TrangThai    string
	LyDoTuChoi   string
	NgayDangKy   time.Time
}

// fakeLopHocPhan mô phỏng bảng `lop_hoc_phan` + `lop_hoc_phan_counter`.
type fakeLopHocPhan struct {
	SoLuongToiDa   int
	SoLuongDaDangKy int
}

// fakeHoSo mô phỏng bảng mới `sinh_vien_ho_so` cần thêm cho TOPSIS.
type fakeHoSo struct {
	UuTienBatBuoc         int
	NguyCoChamTotNghiep   int
	SoKyDaCho             int
	SoLopThayThe          int
}

// slotConLai tương đương fetchSlotConLai() thật trong db.go (SELECT
// so_luong_toi_da - so_luong_da_dang_ky), viết lại đây bằng map thay vì gocql.
func slotConLai(lhp map[string]fakeLopHocPhan, maLopHocPhan string) int {
	l, ok := lhp[maLopHocPhan]
	if !ok {
		return 0
	}
	c := l.SoLuongToiDa - l.SoLuongDaDangKy
	if c < 0 {
		return 0
	}
	return c
}

// dedupe tương đương hàm dedupe() thật trong db.go.
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

// runPendingBatch là bản viết lại THUẦN LOGIC của processPendingBatch() thật
// trong db.go: nhận dữ liệu "ChoXuLy" giả lập, chạy đúng 4 module
// (RuleEngine -> TOPSISBusiness -> AllocationOptimizer), rồi trả về các thay
// đổi trạng thái cần ghi lại -- KHÔNG đụng tới gocql/nats, chỉ để xác minh
// orchestration logic trước khi transcribe sang bản gocql thật.
func runPendingBatch(
	pending []fakeRow,
	hoSo map[string]fakeHoSo,
	lhp map[string]fakeLopHocPhan,
	alreadyRegistered map[string]bool, // key = maSinhVien+"|"+maMonHoc
) (rejected map[string]string, confirmed map[string]string, waitlisted map[string]string) {

	rejected = make(map[string]string)
	confirmed = make(map[string]string)
	waitlisted = make(map[string]string)

	var reqs []StudentRequest
	trackingByKey := make(map[string]string)

	for _, r := range pending {
		hs, ok := hoSo[r.MaSinhVien]
		if !ok {
			hs = fakeHoSo{UuTienBatBuoc: 3, NguyCoChamTotNghiep: 3, SoKyDaCho: 0, SoLopThayThe: 5} // fallback trung tính
		}
		req := StudentRequest{
			StudentID: r.MaSinhVien, CourseID: r.MaMonHoc, SectionID: r.MaLopHocPhan,
			Scores: []float64{
				float64(hs.UuTienBatBuoc), float64(hs.NguyCoChamTotNghiep),
				float64(hs.SoKyDaCho), float64(hs.SoLopThayThe), 0, // C5 xung đột lịch: chưa wire
			},
			SubmittedAt:        r.NgayDangKy,
			PrerequisitesMet:   true,
			CreditLoadOK:       true,
			HasScheduleClash:   false,
			AlreadyRegistered:  alreadyRegistered[r.MaSinhVien+"|"+r.MaMonHoc],
			EligibleForProgram: true,
		}
		reqs = append(reqs, req)
		trackingByKey[r.MaSinhVien+"|"+r.MaMonHoc+"|"+r.MaLopHocPhan] = r.MaDangKy
	}

	ruleEngine := RuleEngine{}
	filtered := ruleEngine.Filter(reqs)
	for k, reason := range filtered.Rejected {
		rejected[trackingByKey[k]] = string(reason)
	}
	if len(filtered.Eligible) == 0 {
		return
	}

	criteria := []Criterion{
		{Name: "mandatory", Type: Benefit},
		{Name: "graduation_delay_risk", Type: Benefit},
		{Name: "semesters_waited", Type: Benefit},
		{Name: "alternative_sections", Type: Cost},
		{Name: "schedule_conflict", Type: Cost},
	}
	ranker := TOPSISBusiness{Criteria: criteria, Weights: WeightConfig{Method: EWM}}
	ranked, err := ranker.RankByCourse(filtered.Eligible)
	if err != nil {
		panic(err) // trong db.go thật: log.Printf rồi return, ở đây panic để test bắt lỗi ngay
	}

	var edges []Edge
	sections := make(map[string]Section)
	for _, pool := range ranked {
		for _, rr := range pool {
			edges = append(edges, Edge{
				StudentID: rr.Request.StudentID, CourseID: rr.Request.CourseID,
				SectionID: rr.Request.SectionID, Score: rr.Score,
			})
			if _, ok := sections[rr.Request.SectionID]; !ok {
				sections[rr.Request.SectionID] = Section{
					ID: rr.Request.SectionID, CourseID: rr.Request.CourseID,
					Capacity: slotConLai(lhp, rr.Request.SectionID),
				}
			}
		}
	}

	optimizer := AllocationOptimizer{Sections: sections}
	result := optimizer.Solve(edges)

	for _, a := range result.Confirmed {
		confirmed[trackingByKey[a.StudentID+"|"+a.CourseID+"|"+a.SectionID]] = a.SectionID
	}
	for _, e := range result.Waitlist {
		waitlisted[trackingByKey[e.StudentID+"|"+e.CourseID+"|"+e.SectionID]] = e.SectionID
	}
	return
}

func TestRunPendingBatch_MatchesRealSchemaShape(t *testing.T) {
	lhp := map[string]fakeLopHocPhan{
		"LHP001": {SoLuongToiDa: 2, SoLuongDaDangKy: 0}, // còn đúng 2 chỗ
	}
	hoSo := map[string]fakeHoSo{
		"SV001": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 5, SoKyDaCho: 4, SoLopThayThe: 1},
		"SV002": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 3, SoKyDaCho: 2, SoLopThayThe: 3},
		"SV003": {UuTienBatBuoc: 3, NguyCoChamTotNghiep: 2, SoKyDaCho: 4, SoLopThayThe: 1},
		"SV004": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 4, SoKyDaCho: 3, SoLopThayThe: 2},
	}
	pending := []fakeRow{
		{MaDangKy: "DK1", MaSinhVien: "SV001", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
		{MaDangKy: "DK2", MaSinhVien: "SV002", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
		{MaDangKy: "DK3", MaSinhVien: "SV003", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
		{MaDangKy: "DK4", MaSinhVien: "SV004", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
	}

	rejected, confirmed, waitlisted := runPendingBatch(pending, hoSo, lhp, map[string]bool{})

	if len(rejected) != 0 {
		t.Fatalf("expected 0 rejected, got %v", rejected)
	}
	if len(confirmed) != 2 {
		t.Fatalf("expected 2 confirmed (capacity=2), got %d: %v", len(confirmed), confirmed)
	}
	if len(waitlisted) != 2 {
		t.Fatalf("expected 2 waitlisted, got %d: %v", len(waitlisted), waitlisted)
	}
	if _, ok := confirmed["DK1"]; !ok {
		t.Fatalf("expected DK1 (SV001, strongest profile) to be confirmed, got confirmed=%v waitlisted=%v", confirmed, waitlisted)
	}
	for dk, section := range confirmed {
		fmt.Printf("[CONFIRM] %s -> %s\n", dk, section)
	}
	for dk, section := range waitlisted {
		fmt.Printf("[WAITLIST] %s -> %s\n", dk, section)
	}
}

func TestRunPendingBatch_AlreadyRegisteredIsRejected(t *testing.T) {
	lhp := map[string]fakeLopHocPhan{"LHP001": {SoLuongToiDa: 5, SoLuongDaDangKy: 0}}
	hoSo := map[string]fakeHoSo{"SV001": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 5, SoKyDaCho: 4, SoLopThayThe: 1}}
	pending := []fakeRow{
		{MaDangKy: "DK1", MaSinhVien: "SV001", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
	}
	alreadyRegistered := map[string]bool{"SV001|MH001": true}

	rejected, confirmed, waitlisted := runPendingBatch(pending, hoSo, lhp, alreadyRegistered)
	if len(confirmed) != 0 || len(waitlisted) != 0 {
		t.Fatalf("expected no confirm/waitlist for a duplicate registration, got confirmed=%v waitlisted=%v", confirmed, waitlisted)
	}
	if reason, ok := rejected["DK1"]; !ok || reason != string(ReasonDuplicateRegistration) {
		t.Fatalf("expected DK1 rejected as duplicate, got %v", rejected)
	}
}

func TestRunPendingBatch_MissingProfileFallsBackNeutralNotCrash(t *testing.T) {
	lhp := map[string]fakeLopHocPhan{"LHP001": {SoLuongToiDa: 1, SoLuongDaDangKy: 0}}
	pending := []fakeRow{
		{MaDangKy: "DK1", MaSinhVien: "SV_NEW", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
	}
	// hoSo rỗng -> SV_NEW không có hồ sơ trong sinh_vien_ho_so
	rejected, confirmed, waitlisted := runPendingBatch(pending, map[string]fakeHoSo{}, lhp, map[string]bool{})
	if len(rejected) != 0 {
		t.Fatalf("expected sinh viên thiếu hồ sơ vẫn được xét (fallback), got rejected=%v", rejected)
	}
	if len(confirmed) != 1 {
		t.Fatalf("expected 1 confirmed via fallback profile, got confirmed=%v waitlisted=%v", confirmed, waitlisted)
	}
}

func TestDedupe(t *testing.T) {
	got := dedupe([]string{"a", "b", "a", "", "c", "b"})
	sort.Strings(got)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}
