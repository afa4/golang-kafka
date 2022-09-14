package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

func main() {
	startClient()
}

var (
	topicForRequest = "is-even-request"
	topicForReply   = "is-even-reply"
	clientId        = uuid.New().String()
)

func startClient() {
	repliesConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "server",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}
	defer repliesConsumer.Close()

	fmt.Printf("Client %s started...\n", clientId)
	repliesConsumer.SubscribeTopics([]string{topicForReply}, nil)
	for {
		message, err := repliesConsumer.ReadMessage(-1) // indefinite wait {"RequesterId":"4bfad623-b465-427c-88ff-e287ff242cf3", "Integer":2, "CreatedAt":1}
		if err != nil {
			continue
		}
		messageKey := string(message.Key)
		if messageKey != clientId {
			fmt.Printf("[SHOULD DELETE] Message on %s: key=%s value=%s\n", message.TopicPartition.String(), string(message.Key), string(message.Value))
			continue
		}
		fmt.Printf("[SHOULD PROCESS] Message on %s: key=%s value=%s\n", message.TopicPartition.String(), string(message.Key), string(message.Value))
	}
}
