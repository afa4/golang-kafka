package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/afa4/golang-kafka/types"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

var (
	topicForRequest = "is-even-request"
	topicForReply   = "is-even-reply"
	clientId        = uuid.New().String()
)

func main() {
	numberOfRequests, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic("Fatal error")
	}
	repliesDurations, err := startClient(numberOfRequests)
	if err != nil {
		panic("Fatal error")
	}

	for e := repliesDurations.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
}

func startClient(numberOfRequests int) (*list.List, error) {
	go requestIsEven(numberOfRequests)

	repliesConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          clientId,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		return nil, err
	}
	defer repliesConsumer.Close()

	repliesConsumer.SubscribeTopics([]string{topicForReply}, nil)
	responsesDurations := list.New()
	for {
		message, err := repliesConsumer.ReadMessage(time.Second * 3)
		if err != nil {
			break
		}
		messageKey := string(message.Key)
		if messageKey != clientId {
			continue
		}
		duration, err := handleReplyMessages(message)
		if err != nil {
			fmt.Println("Handle Message error")
			continue
		}
		responsesDurations.PushBack(duration)
	}
	return responsesDurations, err
}

func requestIsEven(numberOfTimes int) error {
	requestProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	defer requestProducer.Close()
	for i := 0; i < numberOfTimes; i++ {
		encodedRequest, err := buildRandomIsEvenRequest()
		if err != nil {
			panic(err)
		}
		err = requestProducer.Produce(&kafka.Message{
			Key:            []byte(clientId),
			TopicPartition: kafka.TopicPartition{Topic: &topicForRequest},
			Value:          encodedRequest,
		}, nil)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func buildRandomIsEvenRequest() ([]byte, error) {
	return json.Marshal(
		types.IsEvenRequest{
			RequesterId: clientId,
			Integer:     rand.Int31(),
			CreatedAt:   time.Now().UnixMilli(),
		},
	)
}

func handleReplyMessages(message *kafka.Message) (int64, error) {
	reply := types.IsEvenResponse{}
	err := json.Unmarshal(message.Value, &reply)
	if err != nil {
		return 0, err
	}
	return time.Now().UnixMilli() - reply.RequestedAt, err
}
