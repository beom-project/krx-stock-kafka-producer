package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/beomsun1234/krx-stock-collector/krx"
	"github.com/beomsun1234/krx-stock-kafka-producer/properties"
)

func main() {
	p := properties.NewProperties()
	p.SetProperties("producer-properties.yaml")

	k := krx.New(&http.Client{})
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Admin.Timeout = time.Duration(2)

	kafkaClient, err := sarama.NewClient(p.KafkaProperties.Addrs, kafkaConfig)
	if err != nil {
		panic(err)
	}

	producer, err := sarama.NewSyncProducerFromClient(kafkaClient)
	if err != nil {
		panic(err)
	}

	rand.Seed(time.Now().UnixNano())

	/*
		3~5초 마다 전송
	*/
	for {
		fmt.Println("------------start------------------------------------------------------------------------")
		collected_stock_prices := k.GetDailyMarketPrice()
		if collected_stock_prices == nil {
			continue
		}

		_, _, err := producer.SendMessage(genMessage(collected_stock_prices, p.KafkaProperties.Topic))
		if err != nil {
			fmt.Println("Message sent failed")
			continue
		}

		fmt.Println(collected_stock_prices)
		fmt.Println("Message sent successfully")
		fmt.Println("------------end----------------------------------------------------------------------------")
		time.Sleep(time.Duration(rand.Intn(5-2)+2) * time.Second)
	}

}

func genMessage(data []krx.Stock, topic string) *sarama.ProducerMessage {
	msg, _ := json.Marshal(data)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}
}
