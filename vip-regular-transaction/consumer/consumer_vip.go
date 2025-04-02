package main

// func runConsumerVip() {
// 	// This function is called when the mode is consumer_vip
// 	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers": "localhost:9092",
// 		"group.id":          "log-consumer-group",
// 		"auto.offset.reset": "earliest",
// 	})
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer consumer.Close()

// 	topics := "transaction"

// 	consumer.Assign([]kafka.TopicPartition{
// 		{Topic: &topics, Partition: 1},
// 	})

// 	fmt.Println("Consumer is reading from partition 1...")
// 	for {
// 		msg, err := consumer.ReadMessage(-1)
// 		if err == nil {
// 			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
// 		} else {
// 			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
// 		}
// 	}
// }