package redismetric

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "RedisMetricPlugin"

// RedisMetricPlugin là struct chính của plugin
type RedisMetricPlugin struct {
	handle        framework.Handle
	scyllaSession *gocql.Session // ScyllaDB thay PostgreSQL
}

var _ framework.ScorePlugin = &RedisMetricPlugin{}

// Name trả về tên của plugin
func (rmp *RedisMetricPlugin) Name() string {
	return Name
}

// getTopNodesFromDB lấy tên node tốt nhất từ ScyllaDB (không cache)
func (rmp *RedisMetricPlugin) getTopNodesFromDB(ctx context.Context) ([]string, error) {
	// Tính toán trực tiếp từ ScyllaDB mỗi lần gọi
	nodeName, err := CalculateTopNodeFromScylla(ctx, rmp.scyllaSession)
	if err != nil {
		klog.ErrorS(err, "Failed to calculate top node from ScyllaDB")
		return nil, fmt.Errorf("failed to calculate top node: %w", err)
	}

	// Nếu giá trị rỗng
	if nodeName == "" {
		klog.V(4).InfoS("Calculated top node is empty")
		return []string{}, nil
	}

	topNodes := []string{nodeName}
	klog.V(5).InfoS("Calculated top node from ScyllaDB", "node", nodeName)

	return topNodes, nil
}

// isNodeInTopNodes kiểm tra node có nằm trong danh sách top nodes không
func (rmp *RedisMetricPlugin) isNodeInTopNodes(nodeName string, topNodes []string) bool {
	for _, topNode := range topNodes {
		if topNode == nodeName {
			return true
		}
	}
	return false
}

// calculateScore tính điểm dựa trên việc node có trong top list hay không
func (rmp *RedisMetricPlugin) calculateScore(nodeName string, topNodes []string) int64 {
	if rmp.isNodeInTopNodes(nodeName, topNodes) {
		klog.V(5).InfoS("Node is in top list, giving full score", "node", nodeName)
		return 100
	}
	klog.V(5).InfoS("Node is not in top list, giving zero score", "node", nodeName)
	return 0
}

// Score là hàm quan trọng nhất, được gọi để chấm điểm cho từng node
func (rmp *RedisMetricPlugin) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	nodeName := ""
	if n := nodeInfo.Node(); n != nil {
		nodeName = n.Name
	}

	// Lấy danh sách top nodes từ ScyllaDB (không cache)
	topNodes, err := rmp.getTopNodesFromDB(ctx)
	if err != nil {
		klog.ErrorS(err, "Failed to get top nodes, returning score 0", "node", nodeName)
		return 0, nil
	}

	// Nếu không có top nodes nào, trả về 0
	if len(topNodes) == 0 {
		klog.V(4).InfoS("No top nodes found, returning score 0", "node", nodeName)
		return 0, nil
	}

	// Tính điểm
	score := rmp.calculateScore(nodeName, topNodes)
	return score, nil
}

func (rmp *RedisMetricPlugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// New là hàm khởi tạo plugin
func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	// Kết nối ScyllaDB
	cluster := gocql.NewCluster("192.168.24.13", "192.168.24.15", "192.168.24.19")
	cluster.Keyspace = "my_keyspace"
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 2 // Giới hạn connection pool

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("không thể kết nối ScyllaDB: %w", err)
	}

	// Ping test
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := session.Query("SELECT now() FROM system.local").WithContext(pingCtx).Exec(); err != nil {
		session.Close()
		return nil, fmt.Errorf("không thể ping ScyllaDB: %w", err)
	}

	klog.InfoS("RedisMetricPlugin đã kết nối thành công đến ScyllaDB")

	return &RedisMetricPlugin{
		handle:        h,
		scyllaSession: session,
	}, nil
}
