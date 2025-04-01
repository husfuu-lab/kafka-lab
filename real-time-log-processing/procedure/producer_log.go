package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// create producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Gagal membuat producer: %v", err)
	}
	defer producer.Close()

	topic := "logs"

	// simulate sending message log to kafka topic every 2 second
	for i := range 5 {
		logMsg := fmt.Sprintf("ERROR %d: Database connection failed", i)	
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value: 		[]byte(logMsg),
		}, nil)
		if err != nil {
			log.Fatalf("Gagal mengirim message: %v", err)
		}
		log.Println("Message sent: ", logMsg)
		time.Sleep(2 * time.Second)
	}
}