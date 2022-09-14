package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/afa4/golang-kafka/types"
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
		"group.id":          "client",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}
	defer repliesConsumer.Close()

	fmt.Printf("Client %s started...\n", clientId)
	repliesConsumer.SubscribeTopics([]string{topicForReply}, nil)
	go requestSeveralIsEven()
	for {
		message, err := repliesConsumer.ReadMessage(-1) // indefinite wait {"RequesterId":"4bfad623-b465-427c-88ff-e287ff242cf3", "Integer":2, "CreatedAt":1}
		if err != nil {
			continue
		}
		messageKey := string(message.Key)
		if messageKey != clientId {
			fmt.Printf("[NOT PROCESS] Message on %s: key=%s value=%s\n", message.TopicPartition.String(), string(message.Key), string(message.Value))
			continue
		}
		fmt.Printf("[PROCESS] Message on %s: key=%s value=%s\n", message.TopicPartition.String(), string(message.Key), string(message.Value))
	}
}

func requestSeveralIsEven() error {
	requestProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	defer requestProducer.Close()
	for i := 0; i < 100; i++ {
		encodedRequest, err := buildRandomIsEvenRequest()
		if err != nil {
			return err
		}
		err = requestProducer.Produce(&kafka.Message{
			Key:            []byte(clientId),
			TopicPartition: kafka.TopicPartition{Topic: &topicForRequest},
			Value:          encodedRequest,
		}, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func buildRandomIsEvenRequest() ([]byte, error) {
	return json.Marshal(
		types.IsEvenRequest{
			RequesterId: clientId,
			Integer:     rand.Int31(),
			CreatedAt:   time.Now().UnixNano(),
		},
	)
}
