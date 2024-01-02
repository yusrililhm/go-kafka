package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type data struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

var message = []data{
	{
		Id:   1,
		Name: "hani",
	},
	{
		Id:   2,
		Name: "haerin",
	},
	{
		Id:   3,
		Name: "hyein",
	},
	{
		Id:   4,
		Name: "minji",
	},
}

func newProducer() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})

	if err != nil {
		log.Fatal(err.Error())
	}

	defer producer.Close()

	go func() {
		for eachEvent := range producer.Events() {
			switch ev := eachEvent.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Println("error when delivery: ", ev.TopicPartition)
				} else {
					log.Println("delivery success: ", ev.TopicPartition)
				}
			}
		}
	}()

	topic := "orders"

	for _, eachMessage := range message {

		b, _ := json.Marshal(eachMessage)

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &topic,
			},
			Value: b,
		}, nil)
	}

	producer.Flush(2 * 1000)
}

func newConsumer() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "payment",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Fatal(err.Error())
	}

	defer consumer.Close()

	err = consumer.Subscribe("orders", nil)

	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		message, err := consumer.ReadMessage(1 * time.Second)

		if err == nil {
			log.Printf("receive message: %v", string(message.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			log.Printf("error: %v (%v)\n", err, message)
		}
	}
}

func main() {
	newProducer()

	wg := sync.WaitGroup{}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			newConsumer()
			wg.Done()
		}(&wg)
	}

	wg.Wait()
}
