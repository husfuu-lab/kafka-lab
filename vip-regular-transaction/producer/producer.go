package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func createTopic(admin *kafka.AdminClient, topic string, numParts int, replicationFactor int) {
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	results, err := admin.CreateTopics(
		context.Background(),
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numParts,
			ReplicationFactor: replicationFactor,
		}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
	}

	fmt.Printf("Created topic %v\n", results)
}

func main() {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		panic(err)
	}
	defer admin.Close()

	// Create topic transactions with 2 partitions and replication factor 1
	topics := "transactions" 
	numParts := 2
	replicationFactor := 1
	createTopic(admin, topics, numParts, replicationFactor)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	trxTypes := []string{"vip", "regular"}

	for i := range 10 {
		// generate random transaction
		trx := trxTypes[rand.Intn(len(trxTypes))]
		msg := fmt.Sprintf("[%s] This is transaction number %d", trx, i) 

		partition := hashKey(trx, int(numParts))
		fmt.Printf("Key: %s -> Assigned Partition: %d\n", trx, partition)


		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topics, 
				Partition: int32(partition),
		},
			Key: 			[]byte(trx),
			Value:          []byte(msg),
		}, nil)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
		}
		fmt.Printf("Message %s sent to topic %s\n", msg, topics)
		time.Sleep(5 * time.Second)
	}
}

func hashKey(key string, numPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

