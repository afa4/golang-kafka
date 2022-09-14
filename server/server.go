package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	topicForRequest = "is-even-request"
	topicForReply   = "is-even-reply"
)

func main() {
	startServer()
}

func startServer() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "server",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}
	defer consumer.Close()

	fmt.Println("Waiting for messages...")

	responseProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	defer responseProducer.Close()

	consumer.SubscribeTopics([]string{topicForRequest}, nil)
	for {
		message, err := consumer.ReadMessage(-1) // indefinite wait
		if err != nil {
			continue
		}
		handleMessage(responseProducer, string(message.Value))
	}
}

func handleMessage(responseProducer *kafka.Producer, message string) error {
	integer, err := strconv.Atoi(message)
	fmt.Printf("Consumed message from topic is-even-request: value = %d\n", integer)
	if err != nil {
		return err
	}
	responseProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicForReply, Partition: kafka.PartitionAny},
		Value:          []byte(isEven(integer)),
	}, nil)
	return nil
}

func isEven(integer int) string {
	if integer%2 == 0 {
		return "yes"
	} else {
		return "no"
	}
}
