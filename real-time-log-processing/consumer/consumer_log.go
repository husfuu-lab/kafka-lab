package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// create consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "log_consumer", // consumer group
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topic := "logs"
	consumer.Subscribe(topic, nil)

	fmt.Println("Listening for logs...")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Received log: %s\n", string(msg.Value))
		} else {
			fmt.Println("Consumer error:", err)
		}
	}	
}