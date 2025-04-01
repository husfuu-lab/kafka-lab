package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Buat consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "log-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Gagal membuat consumer: %v", err)
	}
	defer consumer.Close()

	topic := "event_logs"
	consumer.SubscribeTopics([]string{topic}, nil)

	fmt.Println("Menunggu pesan dari Kafka...")

	for {
		msg, err := consumer.ReadMessage(-1) // Tunggu pesan masuk
		if err != nil {
			fmt.Printf("Kesalahan membaca pesan: %v\n", err)
			continue
		}

		// show the message
		// Menampilkan struktur pesan
		fmt.Printf("Pesan diterima dari Topic: %s, Partition: %d, Offset: %d\n", *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
		fmt.Printf("Key: %s\n", string(msg.Key))
		fmt.Printf("Value: %s\n", string(msg.Value))

		// Menampilkan headers (jika ada)
		if len(msg.Headers) > 0 {
			fmt.Println("Headers:")
			for _, h := range msg.Headers {
				fmt.Printf("- %s: %s\n", h.Key, string(h.Value))
			}
		}

		fmt.Printf("Timestamp: %v\n", msg.Timestamp)
		fmt.Println("----------------------")
	}
}
