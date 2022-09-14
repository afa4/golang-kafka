package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	startServer()
}

func startServer() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "server",
	})
	defer consumer.Close()

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	fmt.Println("Waiting for messages...")

	isEvenProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer isEvenProducer.Close()

	consumer.SubscribeTopics([]string{"is-even-request"}, nil)
	for {
		message, err := consumer.ReadMessage(-1) // indefinite wait
		if err != nil {
			continue
		}
		isEvenMessage(isEvenProducer, string(message.Value))
	}
}

func isEvenMessage(producer *kafka.Producer, message string) error {
	integer, err := strconv.Atoi(message)
	fmt.Printf("Consumed message from topic is-even-request: value = %d\n", integer)
	if err != nil {
		return err
	}
	var response string
	if integer%2 == 0 {
		response = "yes"
	} else {
		response = "no"
	}
	responseTopic := "is-even-reply"
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &responseTopic, Partition: kafka.PartitionAny},
		Value:          []byte(response),
	}, nil)
	return nil
}
