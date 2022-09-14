package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "server",
	})
	defer c.Close()

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	fmt.Println("Waiting for messages...")

	c.SubscribeTopics([]string{"is-even-request"}, nil)
	for {
		message, err := c.ReadMessage(-1) // indefinite wait
		if err != nil {
			continue
		}
		fmt.Printf("Consumed message from topic %s: value = %s\n",
			*message.TopicPartition.Topic, string(message.Value))
	}
}
