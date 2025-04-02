package main

// func runConsumerRegular() {
// 	// This function is called when the mode is consumer_regular
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
// 		{Topic: &topics, Partition: 0},
// 	})

// 	for {
// 		msg, err := consumer.ReadMessage(-1)
// 		if err == nil {
// 			fmt.Println("Message on", msg.TopicPartition, ":", string(msg.Value))
// 		} else {
// 			fmt.Println("Consumer error:", err)
// 		}
// 	}
// }