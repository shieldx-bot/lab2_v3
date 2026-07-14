package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/redis/go-redis/v9"
)

// ============================================
// CONFIGURATION
// ============================================
type Config struct {
	// ScyllaDB
	ScyllaHosts []string
	Keyspace    string
	DataCenter  string

	// Redis
	RedisWriteAddr string
	RedisReadAddrs []string
	CacheTTL       time.Duration

	// Server
	ServerPort string
}

var config = Config{
	ScyllaHosts:    []string{"192.168.24.2", "192.168.24.3", "192.168.24.4"},
	Keyspace:       "my_keyspace",
	DataCenter:     "datacenter1",
	RedisWriteAddr: "192.168.24.2:6379",
	RedisReadAddrs: []string{"192.168.24.3:6379", "192.168.24.4:6379"},
	CacheTTL:       600 * time.Second,
	ServerPort:     ":4000",
}

// ============================================
// CLIENTS & TYPES
// ============================================
var scyllaCluster *gocql.ClusterConfig
var scyllaSession *gocql.Session

type RedisCluster struct {
	writeClient *redis.Client
	readClients []*redis.Client
	rr          int
	mu          sync.Mutex
}

var redisCluster *RedisCluster

type DBResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
}

type CacheEntry struct {
	Data      interface{} `json:"data"`
	ExpiresAt int64       `json:"expires_at"`
}

// ============================================
// INIT CONNECTIONS
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
		return fmt.Errorf("scylla init error: %w", err)
	}
	scyllaSession = session
	log.Printf("✅ ScyllaDB connected: %v", config.ScyllaHosts)
	return nil
}

func initRedis() error {
	rc := &RedisCluster{}

	// Init Write Client
	w := redis.NewClient(&redis.Options{
		Addr:         config.RedisWriteAddr,
		PoolSize:     100,
		MinIdleConns: 20,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolTimeout:  4 * time.Second,
		MaxRetries:   1,
	})

	if err := w.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("redis write init error: %w", err)
	}
	rc.writeClient = w
	log.Printf("✅ Redis WRITE connected: %s", config.RedisWriteAddr)

	// Init Read Clients
	for _, addr := range config.RedisReadAddrs {
		r := redis.NewClient(&redis.Options{
			Addr:         addr,
			PoolSize:     100,
			MinIdleConns: 20,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
			PoolTimeout:  4 * time.Second,
			MaxRetries:   1,
		})
		if err := r.Ping(context.Background()).Err(); err != nil {
			log.Printf("⚠️ Redis READ %s unavailable: %v", addr, err)
			continue
		}
		rc.readClients = append(rc.readClients, r)
		log.Printf("✅ Redis READ connected: %s", addr)
	}

	if len(rc.readClients) == 0 {
		rc.readClients = []*redis.Client{rc.writeClient}
	}

	redisCluster = rc
	return nil
}

// ============================================
// CACHE HELPERS
// ============================================
func generateCacheKey(queryType string, params map[string]interface{}) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(queryType)
	b.WriteString(":")
	for i, k := range keys {
		if i > 0 {
			b.WriteString("|")
		}
		b.WriteString(k)
		b.WriteString(":")
		v := params[k]
		if vs, ok := v.(string); ok {
			b.WriteString(vs)
		} else {
			fmt.Fprintf(&b, "%v", v)
		}
	}
	return b.String()
}

func getReadClient() *redis.Client {
	redisCluster.mu.Lock()
	defer redisCluster.mu.Unlock()
	client := redisCluster.readClients[redisCluster.rr%len(redisCluster.readClients)]
	redisCluster.rr++
	return client
}

func getFromCache(ctx context.Context, cacheKey string) (interface{}, bool) {
	client := getReadClient()
	data, err := client.Get(ctx, cacheKey).Bytes()
	if err != nil {
		return nil, false
	}

	var entry CacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, false
	}

	if entry.ExpiresAt > 0 && time.Now().UnixNano() > entry.ExpiresAt {
		go redisCluster.writeClient.Del(context.Background(), cacheKey)
		return nil, false
	}

	return entry.Data, true
}

func setToCache(ctx context.Context, cacheKey string, data interface{}, ttl time.Duration) error {
	entry := CacheEntry{
		Data:      data,
		ExpiresAt: time.Now().Add(ttl).UnixNano(),
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return redisCluster.writeClient.Set(ctx, cacheKey, encoded, ttl).Err()
}

func deleteCacheKey(ctx context.Context, cacheKey string) {
	if err := redisCluster.writeClient.Del(ctx, cacheKey).Err(); err != nil {
		log.Printf("⚠️ Cache delete error: %v", err)
	}
}

func invalidateRelatedCaches(ctx context.Context, queryType string, params map[string]interface{}) {
	if maSinhVien, ok := params["maSinhVien"].(string); ok && maSinhVien != "" {
		dangKyKey := generateCacheKey("GET_DANH_SACH_MON_HOC_PHAN_DANG_KY", map[string]interface{}{
			"masinhvien": maSinhVien,
			"dotDangKy":  params["dotDangKy"],
			"hinhThuc":   params["hinhThuc"],
		})
		deleteCacheKey(ctx, dangKyKey)
	}

	if maLopHocPhan, ok := params["maLopHocPhan"].(string); ok && maLopHocPhan != "" {
		detailKey := generateCacheKey("GET_CHI_TIET_LOP_HOC_PHAN", map[string]interface{}{
			"idLopHocPhan": maLopHocPhan,
		})
		deleteCacheKey(ctx, detailKey)
	}

	if tenMonHoc, ok := params["TenMonHoc"].(string); ok && tenMonHoc != "" {
		lhpKey := generateCacheKey("GET_DANH_SACH_LOP_HOC_PHAN", map[string]interface{}{
			"TenMonHoc": tenMonHoc,
		})
		deleteCacheKey(ctx, lhpKey)
	}
}

// ============================================
// DATABASE LOGIC (Trực tiếp từ worker cũ)
// ============================================
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

func handleGetChiTietLopHocPhan(params map[string]interface{}) DBResponse {
	idLopHocPhan := params["idLopHocPhan"].(string)
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

func handleGetDanhSachMonHocPhanDangKy(params map[string]interface{}) DBResponse {
	masinhvien := params["masinhvien"].(string)
	dotDangKy, _ := params["dotDangKy"].(string)
	hinhThuc, _ := params["hinhThuc"].(string)

	q := `SELECT ma_dang_ky, ma_sinh_vien, ho, ten, ma_lop_hoc_phan, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc, ngay_dang_ky, trang_thai FROM dang_ky WHERE ma_sinh_vien = ?`
	args := []interface{}{masinhvien}
	if hinhThuc != "" {
		q += " AND hinh_thuc = ? ALLOW FILTERING"
		args = append(args, hinhThuc)
	}

	iter := scyllaSession.Query(q, args...).Iter()
	var results []map[string]interface{}
	var item struct {
		MaDangKy, MaSinhVien, Ho, Ten, MaLopHocPhan, TenLopHocPhan, MaMonHoc, PhongHoc, ThoiKhoaBieu, HinhThuc, NgayDangKy, TrangThai string
		SoLuongToiDa                                                                                                                  int
	}
	for iter.Scan(&item.MaDangKy, &item.MaSinhVien, &item.Ho, &item.Ten, &item.MaLopHocPhan, &item.TenLopHocPhan, &item.MaMonHoc, &item.PhongHoc, &item.ThoiKhoaBieu, &item.SoLuongToiDa, &item.HinhThuc, &item.NgayDangKy, &item.TrangThai) {
		row := map[string]interface{}{
			"ma_dang_ky": item.MaDangKy, "ma_sinh_vien": item.MaSinhVien, "ho": item.Ho, "ten": item.Ten,
			"ma_lop_hoc_phan": item.MaLopHocPhan, "ten_lop_hoc_phan": item.TenLopHocPhan, "ma_mon_hoc": item.MaMonHoc,
			"phong_hoc": item.PhongHoc, "thoi_khoa_bieu": item.ThoiKhoaBieu, "so_luong_toi_da": item.SoLuongToiDa,
			"hinh_thuc": item.HinhThuc, "ngay_dang_ky": item.NgayDangKy, "trang_thai": item.TrangThai,
		}
		if dotDangKy != "" {
			if dt, err := time.Parse(time.RFC3339, item.NgayDangKy); err == nil && dt.Format("2006-01-02") == dotDangKy {
				results = append(results, row)
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
	_ = fetchCounters(maLopHocPhans, counterMap)

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

func handleGetDanhSachLopHocPhan(params map[string]interface{}) DBResponse {
	tenMonHocRaw := params["TenMonHoc"]
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
	case []string:
		tenMonHocs = append(tenMonHocs, v...)
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
		MaLopHocPhan, TenLopHocPhan, MaMonHoc, PhongHoc, ThoiKhoaBieu, TrangThai, NgayBatDau, NgayKetThuc string
		SoLuongToiDa                                                                                      int
	}
	var row struct {
		MaLopHocPhan, TenLopHocPhan, MaMonHoc, PhongHoc, ThoiKhoaBieu, TrangThai, NgayBatDau, NgayKetThuc string
		SoLuongToiDa                                                                                      int
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
	_ = fetchCounters(maLopHocPhans, counterMap)

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

func handleDangKyMonHoc(params map[string]interface{}) DBResponse {
	maSinhVien := params["maSinhVien"].(string)
	maLopHocPhan := params["maLopHocPhan"].(string)
	hinhThuc := "Chinh quy"
	if v, ok := params["hinhThuc"].(string); ok && v != "" {
		hinhThuc = v
	}

	maDangKy := fmt.Sprintf("DK_%s_%s_%d", maSinhVien, maLopHocPhan, time.Now().UnixNano())
	ngayDangKy := time.Now()

	err := scyllaSession.Query(`INSERT INTO dang_ky (ma_dang_ky, ma_sinh_vien, ma_lop_hoc_phan, ho, ten, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc, ngay_dang_ky, trang_thai, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		maDangKy, maSinhVien, maLopHocPhan, "LoadTest_Ho", "LoadTest_Ten", "LHP_Test", "MH_Test", "Phong_Test", "Lich_Test", 100, hinhThuc, ngayDangKy, "DaDangKy", ngayDangKy, ngayDangKy).Exec()

	if err != nil {
		return DBResponse{Success: false, Error: err.Error()}
	}

	_ = scyllaSession.Query("UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky + 1 WHERE ma_lop_hoc_phan = ?", maLopHocPhan).Exec()

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

func handleHuyDangKy(params map[string]interface{}) DBResponse {
	maSinhVien := params["maSinhVien"].(string)
	maLopHocPhan := params["maLopHocPhan"].(string)

	err := scyllaSession.Query("DELETE FROM dang_ky WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?", maSinhVien, maLopHocPhan).Exec()
	if err != nil {
		return DBResponse{Success: false, Error: err.Error()}
	}

	_ = scyllaSession.Query("UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky - 1 WHERE ma_lop_hoc_phan = ?", maLopHocPhan).Exec()

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
	case []string:
		maLopHocPhans = append(maLopHocPhans, v...)
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

// Router trung tâm điều phối Query
func executeDBQuery(queryType string, params map[string]interface{}) DBResponse {
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
// API HANDLERS
// ============================================
func handleCachedQuery(c *gin.Context, queryType string, params map[string]interface{}) {
	cacheKey := generateCacheKey(queryType, params)

	if cached, ok := getFromCache(c.Request.Context(), cacheKey); ok {
		log.Printf("💾 Cache HIT: %s", queryType)
		c.JSON(http.StatusOK, gin.H{
			"success":   true,
			"data":      cached,
			"fromCache": true,
		})
		return
	}

	log.Printf("🔄 Cache MISS: %s", queryType)

	// Gọi trực tiếp Database thay vì bắn qua NATS
	resp := executeDBQuery(queryType, params)

	if !resp.Success {
		c.JSON(http.StatusOK, resp)
		return
	}

	if resp.Data != nil {
		go func(key string, d interface{}) {
			if err := setToCache(context.Background(), key, d, config.CacheTTL); err != nil {
				log.Printf("⚠️ Cache set error: %v", err)
			}
		}(cacheKey, resp.Data)
	}

	c.JSON(http.StatusOK, resp)
}

func handleMutation(c *gin.Context, queryType string, params map[string]interface{}) {
	resp := executeDBQuery(queryType, params)

	if resp.Success {
		go invalidateRelatedCaches(context.Background(), queryType, params)
	}

	c.JSON(http.StatusOK, resp)
}

// ============================================
// ROUTER SETUP
// ============================================
func setupRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "API Monolith healthy"})
	})

	api := r.Group("/DangKyHocPhan")
	{
		// CÁC API TRA CỨU
		api.GET("/GetChiTietLopHocPhan", func(c *gin.Context) {
			idLopHocPhan := c.Query("idLopHocPhan")
			if idLopHocPhan == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu idLopHocPhan"})
				return
			}
			handleCachedQuery(c, "GET_CHI_TIET_LOP_HOC_PHAN", map[string]interface{}{"idLopHocPhan": idLopHocPhan})
		})

		api.GET("/GetDanhSachLopHocPhan", func(c *gin.Context) {
			tenMonHocs := c.QueryArray("TenMonHoc")
			if len(tenMonHocs) == 0 {
				if single := c.Query("TenMonHoc"); single != "" {
					tenMonHocs = []string{single}
				}
			}
			if len(tenMonHocs) == 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu TenMonHoc"})
				return
			}
			handleCachedQuery(c, "GET_DANH_SACH_LOP_HOC_PHAN", map[string]interface{}{"TenMonHoc": tenMonHocs})
		})

		api.GET("/BatchGetCounters", func(c *gin.Context) {
			maLopHocPhans := c.QueryArray("maLopHocPhans")
			if len(maLopHocPhans) == 0 {
				if single := c.Query("maLopHocPhans"); single != "" {
					maLopHocPhans = []string{single}
				}
			}
			if len(maLopHocPhans) == 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu maLopHocPhans"})
				return
			}

			// Trong kiến trúc nguyên khối, không cần tạo array queries đẩy vào batch, chỉ cần truyền thẳng slice.
			resp := executeDBQuery("BATCH_GET_COUNTERS", map[string]interface{}{"maLopHocPhans": maLopHocPhans})
			c.JSON(http.StatusOK, resp)
		})

		api.GET("/GetDanhSachMonHocPhanDangKy", func(c *gin.Context) {
			maSinhVien := c.Query("masinhvien")
			if maSinhVien == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu masinhvien"})
				return
			}
			handleCachedQuery(c, "GET_DANH_SACH_MON_HOC_PHAN_DANG_KY", map[string]interface{}{
				"masinhvien": maSinhVien,
				"dotDangKy":  c.Query("dotDangKy"),
				"hinhThuc":   c.Query("hinhThuc"),
			})
		})

		// CÁC API ĐĂNG KÝ (MUTATIONS)
		api.POST("/DangKyMonHoc", func(c *gin.Context) {
			var req struct {
				MaSinhVien   string `json:"maSinhVien"`
				MaLopHocPhan string `json:"maLopHocPhan"`
				DotDangKy    string `json:"dotDangKy"`
				HinhThuc     string `json:"hinhThuc"`
			}
			if err := c.ShouldBindJSON(&req); err != nil || req.MaSinhVien == "" || req.MaLopHocPhan == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu thông tin"})
				return
			}
			handleMutation(c, "DANG_KY_MON_HOC", map[string]interface{}{
				"maSinhVien":   req.MaSinhVien,
				"maLopHocPhan": req.MaLopHocPhan,
				"dotDangKy":    req.DotDangKy,
				"hinhThuc":     req.HinhThuc,
			})
		})

		api.POST("/HuyDangKy", func(c *gin.Context) {
			var req struct {
				MaSinhVien   string `json:"maSinhVien"`
				MaLopHocPhan string `json:"maLopHocPhan"`
			}
			if err := c.ShouldBindJSON(&req); err != nil || req.MaSinhVien == "" || req.MaLopHocPhan == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu thông tin"})
				return
			}
			handleMutation(c, "HUY_DANG_KY", map[string]interface{}{
				"maSinhVien":   req.MaSinhVien,
				"maLopHocPhan": req.MaLopHocPhan,
			})
		})
	}

	return r
}

// ============================================
// MAIN
// ============================================
func main() {
	if err := initScylla(); err != nil {
		log.Fatalf("❌ Scylla init failed: %v", err)
	}
	defer scyllaSession.Close()

	if err := initRedis(); err != nil {
		log.Fatalf("❌ Redis init failed: %v", err)
	}

	gin.SetMode(gin.ReleaseMode)
	router := setupRouter()

	srv := &http.Server{
		Addr:              config.ServerPort,
		Handler:           router,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	// Xử lý Graceful Shutdown
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Println("Shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("Server forced shutdown: %v", err)
		}

		if redisCluster != nil {
			redisCluster.writeClient.Close()
			for _, rc := range redisCluster.readClients {
				rc.Close()
			}
		}

		log.Println("Server exited cleanly")
	}()

	log.Printf("🚀 Monolithic API Service listening on %s", config.ServerPort)
	log.Printf("   Cache TTL: %v", config.CacheTTL)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
