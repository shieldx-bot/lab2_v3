package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"sigs.k8s.io/scheduler-plugins/pkg/redismetric/test/cal"
)

func main() {
	fmt.Println("=== BẮT ĐẦU TEST KẾT NỐI & TÍNH TOÁN SCYLLADB ===")

	// 1. Cấu hình kết nối ScyllaDB
	cluster := gocql.NewCluster("192.168.24.2", "192.168.24.3", "192.168.24.4")
	cluster.Keyspace = "my_keyspace"
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.NumConns = 2

	// 2. Khởi tạo Session
	log.Println("Đang kết nối đến cụm ScyllaDB...")
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("❌ Lỗi FATAL: Không thể khởi tạo session ScyllaDB: %v", err)
	}
	// Đảm bảo đóng kết nối khi script kết thúc để giải phóng tài nguyên
	defer session.Close()
	log.Println("✅ Khởi tạo session thành công.")

	// 3. Ping test để kiểm tra DB có thực sự phản hồi không
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := session.Query("SELECT now() FROM system.local").WithContext(pingCtx).Exec(); err != nil {
		log.Fatalf("❌ Lỗi FATAL: Không thể ping ScyllaDB: %v", err)
	}
	log.Println("✅ Ping ScyllaDB thành công. Database đang hoạt động.")

	// 4. Gọi hàm tính toán để test logic
	fmt.Println("--------------------------------------------------")
	ctx := context.Background()

	startTime := time.Now()
	topNode, err := cal.CalculateTopNodeFromScylla(ctx, session)
	duration := time.Since(startTime)

	if err != nil {
		log.Fatalf("❌ Lỗi khi tính toán top node: %v", err)
	}

	// 5. In kết quả
	fmt.Println("--------------------------------------------------")
	if topNode == "" {
		log.Println("⚠️ Cảnh báo: Hàm tính toán chạy thành công nhưng trả về giá trị rỗng.")
	} else {
		log.Printf("🎯 KẾT QUẢ: Tính toán thành công! Top Node được chọn là: [%s]\n", topNode)
	}
	log.Printf("⏱️ Thời gian thực thi hàm tính toán: %v\n", duration)
}
