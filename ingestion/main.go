package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

// 1. Cấu trúc dữ liệu của một sự kiện (Event Schema)
type TrackingEvent struct {
	UserID    string `json:"user_id" binding:"required"`
	SessionID string `json:"session_id" binding:"required"`
	EventType string `json:"event_type" binding:"required"` // view, click, add_to_cart, purchase
	ItemID    string `json:"item_id" binding:"required"`
	Timestamp int64  `json:"timestamp"`
}

// 2. Khởi tạo Kafka Writer
// func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
// 	return &kafka.Writer{
// 		Addr:     kafka.TCP(kafkaURL),
// 		Topic:    topic,
// 		Balancer: &kafka.Hash{}, // Đảm bảo các message cùng Key (UserID) vào cùng 1 partition
// 	}
// }

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.Hash{},

		// 1. Kích hoạt Asynchronous
		// API sẽ quăng message vào RAM rồi báo OK ngay lập tức, thư viện tự gửi ngầm
		Async: true,

		// 2. Batching
		BatchSize:    10000,                 // Gom 10.000 event gửi 1 lần
		BatchTimeout: 10 * time.Millisecond, // or đợi tối đa 10 mili-giây thì xử lý

		// 3. Nén dữ liệu (Tiết kiệm băng thông)
		Compression: kafka.Snappy,
	}
}

func main() {
	kafkaURL := "kafka:9092"
	topic := "raw_user_events"

	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()

	// Khởi tạo Gin router ở chế độ Release (tối ưu hiệu năng)
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	// 3. API Endpoint để nhận dữ liệu
	router.POST("/track", func(c *gin.Context) {
		var event TrackingEvent

		// Validate JSON từ request
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Gắn timestamp hiện tại nếu client không gửi
		if event.Timestamp == 0 {
			event.Timestamp = time.Now().UnixMilli()
		}

		// Chuyển Struct thành chuỗi JSON dạng byte để gửi vào Kafka
		eventBytes, err := json.Marshal(event)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize event"})
			return
		}

		// 4. Đẩy message vào Kafka
		msg := kafka.Message{
			Key:   []byte(event.UserID), // Key rất quan trọng để giữ thứ tự hành vi user
			Value: eventBytes,
		}

		err = writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Printf("Lỗi khi ghi vào Kafka: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish event"})
			return
		}

		// Phản hồi cho client nhanh nhất có thể
		c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Event tracked"})
	})

	// Khởi động server tại cổng 8080
	log.Println("Ingestion Server đang chạy tại http://localhost:8080")
	if err := router.Run(":8080"); err != nil {
		log.Fatalf("Không thể khởi động server: %v", err)
	}
}
