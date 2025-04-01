package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// Buat producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Gagal membuat producer: %v", err)
	}
	defer producer.Close()

	topic := "event_logs"

	// Kirim pesan ke Kafka
	message := "test doang"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)

	if err != nil {
		log.Fatalf("Gagal mengirim pesan: %v", err)
	}

	fmt.Println("Pesan berhasil dikirim:", message)
	producer.Flush(15 * 1000) // Tunggu sampai pesan dikirim
}
