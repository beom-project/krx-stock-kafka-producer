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
		now := generateNowInKorea()
		if !isWithinTimeRange(now) {
			continue
		}

		businessDay, err := k.GetBusinessDay()
		if err != nil {
			continue
		}

		if !isBusinessDay(businessDay, now) {
			continue
		}

		time.Sleep(time.Second)

		fmt.Println("------------start------------------------------------------------------------------------")
		collected_stock_prices := k.GetMarketPriceByDate(businessDay)
		if collected_stock_prices == nil {
			continue
		}

		_, _, err = producer.SendMessage(generateMessage(collected_stock_prices, p.KafkaProperties.Topic))
		if err != nil {
			fmt.Println("Message sent failed")
			continue
		}

		fmt.Println(collected_stock_prices)
		fmt.Println("Message sent successfully")
		fmt.Println("------------end----------------------------------------------------------------------------")
		time.Sleep(time.Duration(rand.Intn(4-2)+2) * time.Second)
	}

}

func generateNowInKorea() time.Time {
	location, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		// 오류 처리
		panic(err)
	}
	timeInKorea := time.Now().In(location)
	return timeInKorea
}

func isWithinTimeRange(timeInKorea time.Time) bool {
	location, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		return false
	}
	// 시작 시간과 종료 시간 설정
	startTime := time.Date(timeInKorea.Year(), timeInKorea.Month(), timeInKorea.Day(), 9, 20, 0, 0, location)
	endTime := time.Date(timeInKorea.Year(), timeInKorea.Month(), timeInKorea.Day(), 15, 20, 0, 0, location)
	// 현재 시간이 시작 시간과 종료 시간 사이에 있는지 확인
	return timeInKorea.After(startTime) && timeInKorea.Before(endTime)
}

func isBusinessDay(businessDay string, timeInKorea time.Time) bool {
	now := timeInKorea.Format("20060102")

	return now == businessDay
}

func generateMessage(data []krx.Stock, topic string) *sarama.ProducerMessage {
	msg, _ := json.Marshal(data)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}
}
