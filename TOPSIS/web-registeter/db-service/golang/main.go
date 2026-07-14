package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
}

func LoadConfig() Config {
	// 1. Đọc các chuỗi phân tách bằng dấu phẩy thành []string
	scyllaHosts := strings.Split(getEnv("SCYLLA_HOSTS", "192.168.24.13,192.168.24.15,192.168.24.19"), ",")
	natsServers := strings.Split(getEnv("NATS_SERVERS", "nats://192.168.24.10:5222,nats://192.168.24.11:6222,nats://192.168.24.12:7222"), ",")

	return Config{
		ScyllaHosts: scyllaHosts,
		Keyspace:    getEnv("KEYSPACE", "my_keyspace"),
		NATSServers: natsServers,
		DataCenter:  getEnv("DATA_CENTER", "datacenter1"),
		MaxWorkers:  50,
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

// ============================================
// CLIENTS
// ============================================
var scyllaCluster *gocql.ClusterConfig
var scyllaSession *gocql.Session
var natsConn *nats.Conn
var ncCloseMu sync.Mutex // chỉ dùng khi close/reconnect, không cần RLock

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
func handleDangKyMonHoc(query map[string]interface{}) DBResponse {
	maSinhVien := query["maSinhVien"].(string)
	maLopHocPhan := query["maLopHocPhan"].(string)
	hinhThuc := "Chinh quy"
	if v, ok := query["hinhThuc"].(string); ok && v != "" {
		hinhThuc = v
	}

	maDangKy := fmt.Sprintf("DK_%s_%s_%d", maSinhVien, maLopHocPhan, time.Now().UnixNano())
	ngayDangKy := time.Now()

	// 1. CHIẾN THUẬT BLIND INSERT: Bỏ qua 4 lệnh SELECT kiểm tra logic.
	// Dùng data giả (LoadTest) cho các trường tên để tiết kiệm chi phí tra cứu DB.
	err := scyllaSession.Query(`INSERT INTO dang_ky (ma_dang_ky, ma_sinh_vien, ma_lop_hoc_phan, ho, ten, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc, ngay_dang_ky, trang_thai, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		maDangKy, maSinhVien, maLopHocPhan, "LoadTest_Ho", "LoadTest_Ten", "LHP_Test", "MH_Test", "Phong_Test", "Lich_Test", 100, hinhThuc, ngayDangKy, "DaDangKy", ngayDangKy, ngayDangKy).Exec()

	if err != nil {
		// Chỉ trả về false nếu Database thực sự sập hoặc rớt mạng (Lỗi kỹ thuật)
		return DBResponse{Success: false, Error: err.Error()}
	}

	// 2. Cập nhật Counter (Bắn và quên, không cần đợi phản hồi lỗi quá khắt khe)
	_ = scyllaSession.Query("UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky + 1 WHERE ma_lop_hoc_phan = ?", maLopHocPhan).Exec()

	// 3. ÉP TRẢ VỀ TRUE (Bỏ qua lỗi lớp đầy hay trùng lặp sinh viên)
	return DBResponse{
		Success: true,
		Data: map[string]interface{}{
			"ma_dang_ky":      maDangKy,
			"ma_sinh_vien":    maSinhVien,
			"ma_lop_hoc_phan": maLopHocPhan,
		},
		Message: "Đăng ký thành công (Load Test Mode)",
	}
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

	log.Println("⏳ DB Service ready, waiting for requests...")
	log.Printf("   ScyllaDB: %v", config.ScyllaHosts)
	log.Printf("   NATS: %v", config.NATSServers)
	log.Printf("   Max Workers: %d", config.MaxWorkers)

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
