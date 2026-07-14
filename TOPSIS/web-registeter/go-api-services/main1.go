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
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

// ============================================
// CONFIGURATION
// ============================================
type Config struct {
	RedisWriteAddr string
	RedisReadAddrs []string
	NATSServers    []string
	NATSTimeout    time.Duration
	CacheTTL       time.Duration
	ServerPort     string
}

func LoadConfig() Config {
	// Parse mảng chuỗi
	redisReadAddrs := strings.Split(getEnv("REDIS_READ_ADDRS", "192.168.24.8:6379,192.168.24.9:6379"), ",")
	natsServers := strings.Split(getEnv("NATS_SERVERS", "nats://192.168.24.10:5222,nats://192.168.24.11:6222,nats://192.168.24.12:7222"), ",")

	// Parse định dạng thời gian (time.Duration)
	natsTimeout, err := time.ParseDuration(getEnv("NATS_TIMEOUT", "8s"))
	if err != nil {
		natsTimeout = 8 * time.Second
	}

	cacheTTL, err := time.ParseDuration(getEnv("CACHE_TTL", "600s"))
	if err != nil {
		cacheTTL = 600 * time.Second
	}

	return Config{
		RedisWriteAddr: getEnv("REDIS_WRITE_ADDR", "192.168.24.5:6379"),
		RedisReadAddrs: redisReadAddrs,
		NATSServers:    natsServers,
		NATSTimeout:    natsTimeout,
		CacheTTL:       cacheTTL,
		ServerPort:     getEnv("SERVER_PORT", ":4000"),
	}
}

// Hàm helper lấy ENV có giá trị mặc định
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
type RedisCluster struct {
	writeClient *redis.Client
	readClients []*redis.Client
	rr          int
	mu          sync.Mutex
}

var redisCluster *RedisCluster
var natsConn *nats.Conn
var ncMu sync.RWMutex

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

type CacheEntry struct {
	Data      interface{} `json:"data"`
	ExpiresAt int64       `json:"expires_at"`
}

// ============================================
// INIT
// ============================================
func initRedis() error {
	rc := &RedisCluster{}

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
		return err
	}
	rc.writeClient = w
	log.Printf("✅ Redis WRITE: %s", config.RedisWriteAddr)

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
		log.Printf("✅ Redis READ: %s", addr)
	}

	if len(rc.readClients) == 0 {
		rc.readClients = []*redis.Client{rc.writeClient}
	}

	redisCluster = rc
	return nil
}

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
		return err
	}
	log.Printf("✅ NATS connected")
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

// ============================================
// NATS REQUESTS
// ============================================
func sendDBRequest(ctx context.Context, queryType string, params map[string]interface{}) (*DBResponse, error) {
	req := DBRequest{QueryType: queryType, Params: params}
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	ncMu.RLock()
	nc := natsConn
	ncMu.RUnlock()
	if nc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}

	msg, err := nc.RequestWithContext(ctx, "db.query", reqData)
	if err != nil {
		return nil, err
	}

	var resp DBResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func sendBatchDBRequest(ctx context.Context, queries []map[string]interface{}) ([]DBResponse, error) {
	type BatchRequest struct {
		Queries []map[string]interface{} `json:"queries"`
	}
	req := BatchRequest{Queries: queries}
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	ncMu.RLock()
	nc := natsConn
	ncMu.RUnlock()
	if nc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}

	msg, err := nc.RequestWithContext(ctx, "db.batch.query", reqData)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Success bool         `json:"success"`
		Results []DBResponse `json:"results"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// ============================================
// CACHE INVALIDATION
// ============================================
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
// HANDLERS
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

	log.Printf("🔄 Cache MISS: %s %v", queryType, params)

	ctx, cancel := context.WithTimeout(c.Request.Context(), config.NATSTimeout)
	defer cancel()

	resp, err := sendDBRequest(ctx, queryType, params)
	if err != nil {
		c.JSON(http.StatusGatewayTimeout, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

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
	ctx, cancel := context.WithTimeout(c.Request.Context(), config.NATSTimeout)
	defer cancel()

	resp, err := sendDBRequest(ctx, queryType, params)
	if err != nil {
		c.JSON(http.StatusGatewayTimeout, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	if resp.Success {
		go invalidateRelatedCaches(context.Background(), queryType, params)
	}

	c.JSON(http.StatusOK, resp)
}

func handleGetDanhSachLopHocPhan(c *gin.Context) {
	// Lấy danh sách TenMonHoc từ URL query string (VD: ?TenMonHoc=Toan&TenMonHoc=Ly)
	tenMonHocs := c.QueryArray("TenMonHoc")

	// Fallback dự phòng nếu client chỉ truyền 1 tham số dạng string thường
	if len(tenMonHocs) == 0 {
		if single := c.Query("TenMonHoc"); single != "" {
			tenMonHocs = []string{single}
		}
	}

	if len(tenMonHocs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu TenMonHoc"})
		return
	}

	params := map[string]interface{}{"TenMonHoc": tenMonHocs}
	cacheKey := generateCacheKey("GET_DANH_SACH_LOP_HOC_PHAN", params)

	if cached, ok := getFromCache(c.Request.Context(), cacheKey); ok {
		log.Printf("💾 Cache HIT: GET_DANH_SACH_LOP_HOC_PHAN")
		c.JSON(http.StatusOK, gin.H{
			"success":   true,
			"data":      cached,
			"fromCache": true,
		})
		return
	}

	log.Printf("🔄 Cache MISS: GET_DANH_SACH_LOP_HOC_PHAN %v", tenMonHocs)

	ctx, cancel := context.WithTimeout(c.Request.Context(), config.NATSTimeout)
	defer cancel()

	resp, err := sendDBRequest(ctx, "GET_DANH_SACH_LOP_HOC_PHAN", params)
	if err != nil {
		c.JSON(http.StatusGatewayTimeout, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

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

func handleGetChiTietLopHocPhan(c *gin.Context) {
	// Lấy tham số từ URL thay vì Body
	idLopHocPhan := c.Query("idLopHocPhan")
	if idLopHocPhan == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu idLopHocPhan"})
		return
	}

	params := map[string]interface{}{"idLopHocPhan": idLopHocPhan}
	handleCachedQuery(c, "GET_CHI_TIET_LOP_HOC_PHAN", params)
}

func handleBatchGetCounters(c *gin.Context) {
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

	ctx, cancel := context.WithTimeout(c.Request.Context(), config.NATSTimeout)
	defer cancel()

	queries := make([]map[string]interface{}, len(maLopHocPhans))
	for i, maLHP := range maLopHocPhans {
		queries[i] = map[string]interface{}{
			"queryType": "BATCH_GET_COUNTERS",
			"params": map[string]interface{}{
				"maLopHocPhans": []string{maLHP},
			},
		}
	}

	results, err := sendBatchDBRequest(ctx, queries)

	if err != nil {
		c.JSON(http.StatusGatewayTimeout, gin.H{"success": false, "error": err.Error()})
		return
	}

	counterMap := make(map[string]int)
	for _, r := range results {
		if r.Success {
			if m, ok := r.Data.(map[string]interface{}); ok {
				for k, v := range m {
					if vi, ok := v.(float64); ok {
						counterMap[k] = int(vi)
					}
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": counterMap})
}

// ============================================
// ROUTES
// ============================================
func setupRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "API healthy"})
	})

	api := r.Group("/DangKyHocPhan")
	{
		// 1. CÁC API TRA CỨU -> DÙNG GET ĐỂ CLOUDFLARE CACHE
		api.GET("/GetChiTietLopHocPhan", handleGetChiTietLopHocPhan)
		api.GET("/GetDanhSachLopHocPhan", handleGetDanhSachLopHocPhan)
		api.GET("/BatchGetCounters", handleBatchGetCounters)

		api.GET("/GetDanhSachMonHocPhanDangKy", func(c *gin.Context) {
			maSinhVien := c.Query("masinhvien")
			dotDangKy := c.Query("dotDangKy")
			hinhThuc := c.Query("hinhThuc")

			if maSinhVien == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu masinhvien"})
				return
			}
			params := map[string]interface{}{
				"masinhvien": maSinhVien,
				"dotDangKy":  dotDangKy,
				"hinhThuc":   hinhThuc,
			}
			handleCachedQuery(c, "GET_DANH_SACH_MON_HOC_PHAN_DANG_KY", params)
		})

		// 2. CÁC API ĐĂNG KÝ (GHI DỮ LIỆU) -> BẮT BUỘC DÙNG POST
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
			params := map[string]interface{}{
				"maSinhVien":   req.MaSinhVien,
				"maLopHocPhan": req.MaLopHocPhan,
				"dotDangKy":    req.DotDangKy,
				"hinhThuc":     req.HinhThuc,
			}
			handleMutation(c, "DANG_KY_MON_HOC", params)
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
			params := map[string]interface{}{
				"maSinhVien":   req.MaSinhVien,
				"maLopHocPhan": req.MaLopHocPhan,
			}
			handleMutation(c, "HUY_DANG_KY", params)
		})
	}

	return r
}

// ============================================
// MAIN
// ============================================
func main() {
	if err := initRedis(); err != nil {
		log.Fatalf("❌ Redis init failed: %v", err)
	}

	if err := initNATS(); err != nil {
		log.Fatalf("❌ NATS init failed: %v", err)
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

		ncMu.Lock()
		if natsConn != nil {
			natsConn.Close()
		}
		ncMu.Unlock()

		if redisCluster != nil {
			redisCluster.writeClient.Close()
			for _, rc := range redisCluster.readClients {
				rc.Close()
			}
		}

		log.Println("Server exited")
	}()

	log.Printf("🚀 Go API Service listening on %s", config.ServerPort)
	log.Printf("   Redis Write: %s", config.RedisWriteAddr)
	log.Printf("   Redis Reads: %v", config.RedisReadAddrs)
	log.Printf("   Cache TTL: %v", config.CacheTTL)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
