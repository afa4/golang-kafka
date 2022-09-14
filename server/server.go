package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type IsEvenResponse struct {
	RequestedAt int64
	IsEven      string
}

type IsEvenRequest struct {
	RequesterId string
	Integer     int
	CreatedAt   int64
}

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
		handleMessage(responseProducer, message)
	}
}

func handleMessage(responseProducer *kafka.Producer, message *kafka.Message) error {
	isEvenRequest := IsEvenRequest{}
	err := json.Unmarshal(message.Value, &isEvenRequest)
	if err != nil {
		return err
	}
	fmt.Printf("Consumed message from topic is-even-request: value = %d\n", isEvenRequest.Integer)
	encodedResponse, err := buildIsEvenResponse(&isEvenRequest)
	if err != nil {
		return err
	}
	err = responseProducer.Produce(&kafka.Message{
		Key:            []byte(isEvenRequest.RequesterId),
		TopicPartition: kafka.TopicPartition{Topic: &topicForReply},
		Value:          encodedResponse,
	}, nil)
	if err != nil {
		return err
	}
	return nil
}

func buildIsEvenResponse(isEvenRequest *IsEvenRequest) ([]byte, error) {
	return json.Marshal(IsEvenResponse{
		RequestedAt: isEvenRequest.CreatedAt,
		IsEven:      isEven(isEvenRequest.Integer),
	})
}

func isEven(integer int) string {
	if integer%2 == 0 {
		return "yes"
	} else {
		return "no"
	}
}
