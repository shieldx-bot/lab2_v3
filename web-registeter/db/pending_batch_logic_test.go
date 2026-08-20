package main

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// fakeRow mô phỏng 1 dòng trong bảng `dang_ky` thật (giữ đủ cột liên quan,
// gồm cả nguyện vọng phụ *_phu mà phân bổ HYBRID sử dụng).
type fakeRow struct {
	MaDangKy         string
	MaSinhVien       string
	MaLopHocPhan     string
	TenLopHocPhan    string
	MaMonHoc         string
	PhongHoc         string
	ThoiKhoaBieu     string
	SoLuongToiDa     int
	MaLopHocPhanPhu  string
	TenLopHocPhanPhu string
	PhongHocPhu      string
	ThoiKhoaBieuPhu  string
	SoLuongToiDaPhu  int
	TrangThai        string
	LyDoTuChoi       string
	NgayDangKy       time.Time
}

// fakeLopHocPhan mô phỏng bảng `lop_hoc_phan` + `lop_hoc_phan_counter`.
type fakeLopHocPhan struct {
	SoLuongToiDa    int
	SoLuongDaDangKy int
}

// fakeHoSo mô phỏng bảng `sinh_vien_ho_so` -- 9 cột tiêu chí C1-C6, C8-C10
// (C7 schedule_conflict được tính từ thoi_khoa_bieu, không lưu cột).
type fakeHoSo struct {
	UuTienBatBuoc         int
	NguyCoChamTotNghiep   int
	SoKyDaCho             int
	SoLanDangKyThatBai    int
	SoTinChiTichLuy       int
	KhoiLuongHocKyHienTai int
	MucPhuHopNguyenVong   int
	SoLopThayThe          int
	KhaNangMoThemLop      int
}

// scores10 dựng vector 10 tiêu chí đúng thứ tự topsisCriteria (C7 = 0 mặc định).
func scores10(hs fakeHoSo) []float64 {
	return []float64{
		float64(hs.UuTienBatBuoc), float64(hs.NguyCoChamTotNghiep),
		float64(hs.SoKyDaCho), float64(hs.SoLanDangKyThatBai),
		float64(hs.SoTinChiTichLuy), float64(hs.KhoiLuongHocKyHienTai),
		0, float64(hs.MucPhuHopNguyenVong), float64(hs.SoLopThayThe),
		float64(hs.KhaNangMoThemLop),
	}
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

// runPendingBatch là bản viết lại THUẦN LOGIC của processPendingBatch() thật
// trong db.go: nhận dữ liệu "ChoXuLy" giả lập, chạy đúng 4 module
// (RuleEngine -> TOPSISBusiness 10 tiêu chí -> AllocationOptimizer HYBRID với
// lớp phụ), rồi trả về các thay đổi trạng thái cần ghi lại -- KHÔNG đụng tới
// gocql/nats, chỉ để xác minh orchestration logic trước khi transcribe.
func runPendingBatch(
	pending []fakeRow,
	hoSo map[string]fakeHoSo,
	lhp map[string]fakeLopHocPhan,
	alreadyRegistered map[string]bool, // key = maSinhVien+"|"+maMonHoc
	confirmedSchedules map[string][]string, // key = maSinhVien -> thoi_khoa_bieu các lớp ĐÃ xác nhận
) (rejected map[string]string, confirmed map[string]string, waitlisted map[string]string) {

	rejected = make(map[string]string)
	confirmed = make(map[string]string)
	waitlisted = make(map[string]string)

	var reqs []StudentRequest
	trackingByKey := make(map[string]string)
	primaryRowByCourse := make(map[string]fakeRow)

	for _, r := range pending {
		hs, ok := hoSo[r.MaSinhVien]
		if !ok {
			hs = fakeHoSo{UuTienBatBuoc: 3, NguyCoChamTotNghiep: 3, SoKyDaCho: 0,
				SoLanDangKyThatBai: 0, SoTinChiTichLuy: 70, KhoiLuongHocKyHienTai: 16,
				MucPhuHopNguyenVong: 3, SoLopThayThe: 3, KhaNangMoThemLop: 3} // fallback trung tính
		}
		sc := scores10(hs)
		sc[6] = float64(countScheduleConflicts(r.ThoiKhoaBieu, confirmedSchedules[r.MaSinhVien]))
		reqs = append(reqs, StudentRequest{
			StudentID: r.MaSinhVien, CourseID: r.MaMonHoc, SectionID: r.MaLopHocPhan,
			Scores:             sc,
			SubmittedAt:        r.NgayDangKy,
			PrerequisitesMet:   true,
			CreditLoadOK:       true,
			HasScheduleClash:   false,
			AlreadyRegistered:  alreadyRegistered[r.MaSinhVien+"|"+r.MaMonHoc],
			EligibleForProgram: true,
		})
		key := r.MaSinhVien + "|" + r.MaMonHoc
		trackingByKey[key+"|"+r.MaLopHocPhan] = r.MaDangKy
		primaryRowByCourse[key] = r
	}

	ruleEngine := RuleEngine{}
	filtered := ruleEngine.Filter(reqs)
	for k, reason := range filtered.Rejected {
		rejected[trackingByKey[k]] = string(reason)
	}
	if len(filtered.Eligible) == 0 {
		return
	}

	cr := []Criterion{
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
	ranker := TOPSISBusiness{Criteria: cr, Weights: WeightConfig{Method: EWM}}
	ranked, err := ranker.RankByCourse(filtered.Eligible)
	if err != nil {
		panic(err) // trong db.go thật: log.Printf rồi return, ở đây panic để test bắt lỗi ngay
	}

	var edges []Edge
	sections := make(map[string]Section)
	addEdge := func(studentID, courseID, sectionID string, score float64) {
		if sectionID == "" {
			return
		}
		edges = append(edges, Edge{StudentID: studentID, CourseID: courseID, SectionID: sectionID, Score: score})
		if _, ok := sections[sectionID]; !ok {
			sections[sectionID] = Section{ID: sectionID, CourseID: courseID, Capacity: slotConLai(lhp, sectionID)}
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

	optimizer := AllocationOptimizer{Sections: sections}
	result := optimizer.Solve(edges)

	handled := make(map[string]bool)
	for _, a := range result.Confirmed {
		key := a.StudentID + "|" + a.CourseID
		row := primaryRowByCourse[key]
		finalSection := a.SectionID
		if a.SectionID == row.MaLopHocPhan {
			finalSection = a.SectionID // lớp chính
		} else {
			// hybrid: chuyển dòng dang_ky sang lớp phụ
			row.MaLopHocPhan = a.SectionID
			row.TenLopHocPhan = row.TenLopHocPhanPhu
			row.PhongHoc = row.PhongHocPhu
			row.ThoiKhoaBieu = row.ThoiKhoaBieuPhu
			row.SoLuongToiDa = row.SoLuongToiDaPhu
			primaryRowByCourse[key] = row
		}
		confirmed[row.MaDangKy] = finalSection
		handled[key] = true
	}
	for _, e := range result.Waitlist {
		key := e.StudentID + "|" + e.CourseID
		if handled[key] {
			continue // đã được xếp lớp phụ -> không đẩy vào chờ
		}
		row := primaryRowByCourse[key]
		waitlisted[row.MaDangKy] = row.MaLopHocPhan
		handled[key] = true
	}
	return
}

func TestRunPendingBatch_MatchesRealSchemaShape(t *testing.T) {
	lhp := map[string]fakeLopHocPhan{
		"LHP001": {SoLuongToiDa: 2, SoLuongDaDangKy: 0}, // còn đúng 2 chỗ
		"LHP002": {SoLuongToiDa: 5, SoLuongDaDangKy: 0}, // lớp thay thế còn nhiều chỗ
	}
	hoSo := map[string]fakeHoSo{
		"SV001": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 5, SoKyDaCho: 4, SoLanDangKyThatBai: 2, SoTinChiTichLuy: 120, KhoiLuongHocKyHienTai: 18, MucPhuHopNguyenVong: 5, SoLopThayThe: 1, KhaNangMoThemLop: 1},
		"SV002": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 3, SoKyDaCho: 2, SoLanDangKyThatBai: 0, SoTinChiTichLuy: 90, KhoiLuongHocKyHienTai: 15, MucPhuHopNguyenVong: 4, SoLopThayThe: 2, KhaNangMoThemLop: 2},
		"SV003": {UuTienBatBuoc: 3, NguyCoChamTotNghiep: 2, SoKyDaCho: 4, SoLanDangKyThatBai: 0, SoTinChiTichLuy: 60, KhoiLuongHocKyHienTai: 12, MucPhuHopNguyenVong: 3, SoLopThayThe: 1, KhaNangMoThemLop: 3},
		"SV004": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 4, SoKyDaCho: 3, SoLanDangKyThatBai: 1, SoTinChiTichLuy: 100, KhoiLuongHocKyHienTai: 17, MucPhuHopNguyenVong: 5, SoLopThayThe: 2, KhaNangMoThemLop: 2},
	}
	pending := []fakeRow{
		{MaDangKy: "DK1", MaSinhVien: "SV001", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
		{MaDangKy: "DK2", MaSinhVien: "SV002", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
		{MaDangKy: "DK3", MaSinhVien: "SV003", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
		{MaDangKy: "DK4", MaSinhVien: "SV004", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
	}

	rejected, confirmed, waitlisted := runPendingBatch(pending, hoSo, lhp, map[string]bool{}, map[string][]string{})

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

// TestRunPendingBatch_HybridMovesToAltWhenPrimaryFull là test TRỌNG TÂM cho
// phân bổ HYBRID: lớp chính LHP001 chỉ còn 1 chỗ, 2 sinh viên đều khai nguyện
// vọng phụ LHP002. Sinh viên mạnh hơn được giữ lớp chính; sinh viên còn lại
// phải được xếp sang LHP002 (dòng dang_ky bị "redirect") chứ KHÔNG bị đẩy vào
// danh sách chờ.
func TestRunPendingBatch_HybridMovesToAltWhenPrimaryFull(t *testing.T) {
	lhp := map[string]fakeLopHocPhan{
		"LHP001": {SoLuongToiDa: 1, SoLuongDaDangKy: 0}, // lớp chính: 1 chỗ
		"LHP002": {SoLuongToiDa: 5, SoLuongDaDangKy: 0}, // lớp phụ: dư chỗ
	}
	hoSo := map[string]fakeHoSo{
		"SV1": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 5, SoKyDaCho: 4, SoLanDangKyThatBai: 2, SoTinChiTichLuy: 130, KhoiLuongHocKyHienTai: 18, MucPhuHopNguyenVong: 5, SoLopThayThe: 1, KhaNangMoThemLop: 1},
		"SV2": {UuTienBatBuoc: 4, NguyCoChamTotNghiep: 2, SoKyDaCho: 1, SoLanDangKyThatBai: 0, SoTinChiTichLuy: 80, KhoiLuongHocKyHienTai: 14, MucPhuHopNguyenVong: 3, SoLopThayThe: 2, KhaNangMoThemLop: 2},
	}
	pending := []fakeRow{
		{
			MaDangKy: "DK1", MaSinhVien: "SV1", MaLopHocPhan: "LHP001",
			TenLopHocPhan: "Toan Cao Cap 1", MaMonHoc: "MH001", ThoiKhoaBieu: "T2|07:00-09:00",
			MaLopHocPhanPhu: "LHP002", TenLopHocPhanPhu: "Toan Cao Cap 2",
			PhongHocPhu: "R204", ThoiKhoaBieuPhu: "T4|07:00-09:00", SoLuongToiDaPhu: 5,
			NgayDangKy: time.Now(),
		},
		{
			MaDangKy: "DK2", MaSinhVien: "SV2", MaLopHocPhan: "LHP001",
			TenLopHocPhan: "Toan Cao Cap 1", MaMonHoc: "MH001", ThoiKhoaBieu: "T2|07:00-09:00",
			MaLopHocPhanPhu: "LHP002", TenLopHocPhanPhu: "Toan Cao Cap 2",
			PhongHocPhu: "R204", ThoiKhoaBieuPhu: "T4|07:00-09:00", SoLuongToiDaPhu: 5,
			NgayDangKy: time.Now(),
		},
	}

	_, confirmed, waitlisted := runPendingBatch(pending, hoSo, lhp, map[string]bool{}, map[string][]string{})

	if len(confirmed) != 2 {
		t.Fatalf("expected cả 2 sinh viên được xếp chỗ (1 chính + 1 phụ), got confirmed=%v", confirmed)
	}
	if len(waitlisted) != 0 {
		t.Fatalf("expected 0 waitlisted nhờ hybrid sang lớp phụ, got %v", waitlisted)
	}
	if sec, ok := confirmed["DK1"]; !ok || sec != "LHP001" {
		t.Fatalf("expected SV1 (mạnh hơn) giữ LHP001, got confirmed=%v", confirmed)
	}
	if sec, ok := confirmed["DK2"]; !ok || sec != "LHP002" {
		t.Fatalf("expected SV2 được HYBRID chuyển sang LHP002, got confirmed=%v", confirmed)
	}
}

func TestRunPendingBatch_AlreadyRegisteredIsRejected(t *testing.T) {
	lhp := map[string]fakeLopHocPhan{"LHP001": {SoLuongToiDa: 5, SoLuongDaDangKy: 0}}
	hoSo := map[string]fakeHoSo{"SV001": {UuTienBatBuoc: 5, NguyCoChamTotNghiep: 5, SoKyDaCho: 4, SoLanDangKyThatBai: 1, SoTinChiTichLuy: 100, KhoiLuongHocKyHienTai: 16, MucPhuHopNguyenVong: 5, SoLopThayThe: 1, KhaNangMoThemLop: 1}}
	pending := []fakeRow{
		{MaDangKy: "DK1", MaSinhVien: "SV001", MaLopHocPhan: "LHP001", MaMonHoc: "MH001", TrangThai: "ChoXuLy", NgayDangKy: time.Now()},
	}
	alreadyRegistered := map[string]bool{"SV001|MH001": true}

	rejected, confirmed, waitlisted := runPendingBatch(pending, hoSo, lhp, alreadyRegistered, map[string][]string{})
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
	rejected, confirmed, waitlisted := runPendingBatch(pending, map[string]fakeHoSo{}, lhp, map[string]bool{}, map[string][]string{})
	if len(rejected) != 0 {
		t.Fatalf("expected sinh viên thiếu hồ sơ vẫn được xét (fallback), got rejected=%v", rejected)
	}
	if len(confirmed) != 1 {
		t.Fatalf("expected 1 confirmed via fallback profile, got confirmed=%v waitlisted=%v", confirmed, waitlisted)
	}
}

func TestScheduleParsingAndOverlap(t *testing.T) {
	a := "T2|07:00-09:00;T4|07:00-09:00"
	b := "T4|08:00-10:00" // trùng T4, chồng 08:00-09:00
	c := "T4|09:00-10:00" // T4 nhưng không chồng (bắt đầu đúng lúc a kết thúc)
	d := "T5|07:00-09:00" // khác ngày
	e := "T3|10:00-12:00" // không liên quan

	if got := countScheduleConflicts(a, []string{b}); got != 1 {
		t.Fatalf("a vs b: expected 1 conflict, got %d", got)
	}
	if got := countScheduleConflicts(a, []string{c}); got != 0 {
		t.Fatalf("a vs c (tiếp giáp, không chồng): expected 0, got %d", got)
	}
	if got := countScheduleConflicts(a, []string{d}); got != 0 {
		t.Fatalf("a vs d (khác ngày): expected 0, got %d", got)
	}
	if got := countScheduleConflicts(a, []string{b, e, d}); got != 1 {
		t.Fatalf("a vs [b, e, d]: expected 1, got %d", got)
	}
	if got := countScheduleConflicts("garbage", []string{b}); got != 0 {
		t.Fatalf("chuỗi không parse được: expected 0, got %d", got)
	}
	if got := countScheduleConflicts("", []string{b}); got != 0 {
		t.Fatalf("chuỗi rỗng: expected 0, got %d", got)
	}
	// định dạng tiếng Anh (Mon/Tue/...) cũng nên hiểu
	if got := countScheduleConflicts("Mon|07:00-09:00", []string{"Monday|08:00-10:00"}); got != 1 {
		t.Fatalf("Mon vs Monday: expected 1, got %d", got)
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
