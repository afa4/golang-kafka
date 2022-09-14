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
			// Errors are informational and automatically handled by the consumer
			continue
		}
		fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
			*message.TopicPartition.Topic, string(message.Key), string(message.Value))
	}
}
