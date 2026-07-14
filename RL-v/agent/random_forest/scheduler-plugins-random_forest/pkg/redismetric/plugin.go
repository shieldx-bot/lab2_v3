package redismetric

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	// Vẫn sử dụng go-redis vì Dragonfly tương thích 100% với giao thức của Redis
	"github.com/redis/go-redis/v9"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "RedisMetricPlugin"

// CacheItem là một mục trong bộ nhớ đệm cục bộ
type CacheItem struct {
	TopNodes  []string
	ExpiresAt time.Time
}

// RedisMetricPlugin là struct chính của plugin
type RedisMetricPlugin struct {
	handle   framework.Handle
	dbClient *sql.DB
	cache    sync.Map // Bộ nhớ đệm an toàn cho goroutine
	cacheTTL time.Duration
}

var _ framework.ScorePlugin = &RedisMetricPlugin{}

// New tạo một instance mới của RedisMetricPlugin
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &RedisMetricPlugin{
		handle:   h,
		cacheTTL: 30 * time.Second,
	}, nil
}

// Name trả về tên của plugin, dùng để đăng ký
func (rmp *RedisMetricPlugin) Name() string {
	return Name
}

// ScoreExtensions trả về nil vì plugin này không cần normalize scores
func (rmp *RedisMetricPlugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// BUG: Global variables rdb and db are declared but never used, and can cause confusion
var rdb *redis.Client
var db *sql.DB

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// --- CHỈ THÊM 3 BIẾN NÀY ĐỂ CHỨA 2 IP DRAGONFLY ---
var (
	dfAddrs = []string{"192.168.24.3:6379", "192.168.24.4:6379"}
	dfIdx   int
	dfMu    sync.Mutex
)

func CalculateTopNodeFromDB(ctx context.Context) (string, error) {

	godotenv.Load()

	str := "node"
	// Create a context with timeout to avoid hanging connections
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// --- CHỈ SỬA CHỖ CONNECT NÀY ---
	// Lấy luân phiên 1 trong 2 IP Dragonfly
	dfMu.Lock()
	dragonflyAddr := dfAddrs[dfIdx]
	dfIdx = (dfIdx + 1) % len(dfAddrs)
	dfMu.Unlock()

	// Configure client để kết nối tới server Dragonfly
	dfClient := redis.NewClient(&redis.Options{
		Addr:     dragonflyAddr, // Dragonfly server address (192.168.24.2 hoặc 192.168.24.6)
		Password: "",            // No password set
		DB:       0,             // Default DB
	})
	defer dfClient.Close()
	// -------------------------------

	// Test connection with PING
	pong, err := dfClient.Ping(ctx).Result()
	if err != nil {
		return "", fmt.Errorf("could not connect to Dragonfly: %w", err)
	}

	fmt.Println("Connected to Dragonfly (random_forest):", pong)

	// find the best index and format node name
	// bestIndex := Score(decision_matrix)
	index, err := dfClient.Get(ctx, "random_forest_TOP").Int()
	if err != nil {
		if err == redis.Nil {
			index = 0
		} else {
			return "", err
		}
	}
	// Fix: Node names are multi-node-demo-worker, multi-node-demo-worker2, multi-node-demo-worker3, etc.
	// For index 0: no node (return empty)
	// For index 1: multi-node-demo-worker
	// For index 2: multi-node-demo-worker2
	// For index 3: multi-node-demo-worker3
	// For index 4: multi-node-demo-worker4
	// For index 5: multi-node-demo-worker5
	var bestNodeName string
	if index == 0 {
		bestNodeName = ""
	} else if index == 1 {
		bestNodeName = str
	} else {
		bestNodeName = fmt.Sprintf("%s%d", str, index)
	}

	return bestNodeName, nil
}

func calculateScore(nodeName string, topNodes string) int64 {
	if nodeName == topNodes {
		return 100
	}
	return 0
}

// Score là hàm quan trọng nhất, được gọi để chấm điểm cho từng node
func (rmp *RedisMetricPlugin) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	nodeName := ""
	if n := nodeInfo.Node(); n != nil {
		nodeName = n.Name
	}

	// Lấy danh sách top nodes
	topNodes, err := CalculateTopNodeFromDB(ctx)
	if err != nil {
		klog.ErrorS(err, "Failed to get top nodes from DB, returning score 0", "node", nodeName)
		return 0, nil
	}

	if len(topNodes) == 0 {
		klog.V(4).InfoS("No top nodes found, returning score 0", "node", nodeName)
		return 0, nil
	}

	// Tính điểm
	score := calculateScore(nodeName, topNodes)
	klog.V(4).InfoS("Calculated score", "node", nodeName, "score", score)
	return score, nil
}
