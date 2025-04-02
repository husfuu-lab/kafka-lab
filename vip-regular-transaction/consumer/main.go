package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	mode := os.Args[1] // Get the mode from the command line arguments
	if mode == "consumer_vip" {
		runConsumerVip()
	} else if mode == "consumer_regular" {
		runConsumerRegular()
	} else {
		panic("Invalid mode")
	}
}

func runConsumerRegular() {
	// This function is called when the mode is consumer_regular
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "transaction-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topics := "transactions"

	consumer.Assign([]kafka.TopicPartition{
		{Topic: &topics, Partition: 0},
	})

	fmt.Println("Consumer is reading from partition 0...")
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Println("Message on", msg.TopicPartition, ":", string(msg.Value))
		} else {
			fmt.Println("Consumer error:", err)
		}
	}
}

func runConsumerVip() {
	// This function is called when the mode is consumer_vip
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "transaction-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	topics := "transactions"

	consumer.Assign([]kafka.TopicPartition{
		{Topic: &topics, Partition: 1},
	})

	fmt.Println("Consumer is reading from partition 1...")
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}