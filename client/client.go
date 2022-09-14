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
	go requestSeveralIsEven()
	repliesConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          clientId,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}
	defer repliesConsumer.Close()

	fmt.Printf("Client %s started...\n", clientId)
	repliesConsumer.SubscribeTopics([]string{topicForReply}, nil)
	counter := 0
	for {
		message, err := repliesConsumer.ReadMessage(time.Second * 5)
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		messageKey := string(message.Key)
		if messageKey != clientId {
			continue
		}
		handleReplyMessages(message)
		counter++
	}
	fmt.Printf("[PROCESSED MESSAGES] %d\n", counter)
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
			continue
			//panic(err)
		}
		err = requestProducer.Produce(&kafka.Message{
			Key:            []byte(clientId),
			TopicPartition: kafka.TopicPartition{Topic: &topicForRequest},
			Value:          encodedRequest,
		}, nil)
		if err != nil {
			continue
			//panic(err)
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

func handleReplyMessages(message *kafka.Message) error {
	reply := types.IsEvenResponse{}
	err := json.Unmarshal(message.Value, &reply)
	if err != nil {
		return err
	}
	fmt.Println(reply.RequestedAt - time.Now().UnixNano())
	return nil
}
