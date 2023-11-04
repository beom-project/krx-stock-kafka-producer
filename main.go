package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"github.com/IBM/sarama"
	"github.com/beomsun1234/krx-stock-collector/krx"
	"github.com/beomsun1234/krx-stock-kafka-producer/properties"
)

type Checker struct {
	I atomic.Uint32
}

var (
	location *time.Location
	checker  *Checker
	db       *sql.DB
)

func main() {
	var error error

	p := properties.NewProperties()
	p.SetProperties("producer-properties.yaml")

	dbConnectionInfo := p.MysqlProperties.Username + ":" + p.MysqlProperties.Password + "@tcp(" + p.MysqlProperties.Addr + ")" + "/" + p.MysqlProperties.DBName
	db, error = sql.Open("mysql", dbConnectionInfo)
	if error != nil {
		panic(error)
	}

	fmt.Println("db 연결 성공")
	defer db.Close()

	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	checker = &Checker{atomic.Uint32{}}

	k := krx.New(&http.Client{})

	location, error = time.LoadLocation("Asia/Seoul")
	if error != nil {
		panic(error)
	}

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
			fmt.Println(err)
			delay(time.Minute * 30)
			continue
		}

		if !isBusinessDay(businessDay, now) {
			fmt.Println("현재 영업일이 아닙니다.")
			delay(time.Hour * 3)
			continue
		}

		delay(time.Second)

		collected_stock_prices := k.GetMarketPriceByDate(businessDay)
		if collected_stock_prices == nil {
			fmt.Println("krx data nil")
			delay(2 * time.Second)
			continue
		}

		if !checkColumn(collected_stock_prices[0].ClosePrice) {
			fmt.Println("잘못된 형식입니다. 메시지를 전송하지 않습니다.")
			delay(2 * time.Second)
			continue
		}

		fmt.Println("------------start------------------------------------------------------------------------")

		fmt.Println("total = ", len(collected_stock_prices), ", first data = ", collected_stock_prices[0])

		fmt.Println("------------end----------------------------------------------------------------------------")

		fmt.Printf("now : %s  |   business day : %s \n", now, businessDay)

		go saveStockOnceADay(now, collected_stock_prices, businessDay)

		_, _, err = producer.SendMessage(generateMessage(collected_stock_prices, p.KafkaProperties.Topic))
		if err != nil {
			fmt.Println("Message sent failed")
			delay(time.Duration(rand.Intn(4-2)+2) * time.Second)
			continue
		}

		fmt.Println("Message sent successfully")

		delay(time.Duration(rand.Intn(4-2)+2) * time.Second)
	}

}

func generateNowInKorea() time.Time {
	timeInKorea := time.Now().In(location)
	return timeInKorea
}

func isWithinTimeRange(timeInKorea time.Time) bool {
	// 시작 시간과 종료 시간 설정
	startTime := time.Date(timeInKorea.Year(), timeInKorea.Month(), timeInKorea.Day(), 9, 20, 0, 0, location)
	endTime := time.Date(timeInKorea.Year(), timeInKorea.Month(), timeInKorea.Day(), 15, 21, 0, 0, location)
	// 현재 시간이 시작 시간과 종료 시간 사이에 있는지 확인
	return timeInKorea.After(startTime) && timeInKorea.Before(endTime)
}

func isBusinessDay(businessDay string, timeInKorea time.Time) bool {
	now := timeInKorea.Format("20060102")

	return now == businessDay
}

func checkColumn(column string) bool {
	return column != "KOSPI"

}

func generateMessage(data []krx.Stock, topic string) *sarama.ProducerMessage {
	msg, _ := json.Marshal(data)

	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}
}

func delay(delayTime time.Duration) {
	time.Sleep(delayTime)
}

func saveStockOnceADay(now time.Time, data []krx.Stock, businessDay string) {
	if !isEndBusinessTime(now) {
		return
	}

	n := convertTimeToInt32(now)
	c := checker.I.Load()

	if c == 0 {
		c = n
	}

	if c != n {
		return
	}
	fmt.Println("bulk insert")
	/*
		bulk insert
	*/
	bulkInsertStocks(data, businessDay)

	checker.setNextEndBusinessTime(now)
}

func isEndBusinessTime(nowTime time.Time) bool {
	endBusinessTime := getEndBusinessTime(nowTime)
	end := endBusinessTime.Format("200601021504")
	now := nowTime.Format("200601021504")
	return now == end
}

func convertTimeToInt32(time time.Time) uint32 {
	t := time.Format("200601021504")
	int_time, err := strconv.Atoi(t)
	if err != nil {
		fmt.Println(err)
		return uint32(00000001)
	}

	return uint32(int_time)
}

func (c *Checker) setNextEndBusinessTime(now time.Time) {
	tomorrow := now.Add(24 * time.Hour)
	endBusinessTime := getEndBusinessTime(tomorrow)
	c.I.Swap(convertTimeToInt32(endBusinessTime))
}

func getEndBusinessTime(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), 15, 20, 0, 0, location)
}

func bulkInsertStocks(data []krx.Stock, businessDay string) {
	tx, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		return
	}
	valueStrings := make([]string, 0, len(data))
	valueArgs := make([]interface{}, 0, len(data)*15)
	for _, stock := range data {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, stock.Ticker)
		valueArgs = append(valueArgs, businessDay)
		valueArgs = append(valueArgs, stock.Name)
		valueArgs = append(valueArgs, "KOSPI")
		valueArgs = append(valueArgs, getChangeCode(stock.FluctuationRange))
		valueArgs = append(valueArgs, stock.FluctuationRange)
		valueArgs = append(valueArgs, stock.FluctuationRate)
		valueArgs = append(valueArgs, stock.OpenPrice)
		valueArgs = append(valueArgs, stock.HighestPrice)
		valueArgs = append(valueArgs, stock.LowestPrice)
		valueArgs = append(valueArgs, stock.ClosePrice)
		valueArgs = append(valueArgs, stock.Volume)
		valueArgs = append(valueArgs, stock.TradingValue)
		valueArgs = append(valueArgs, stock.MarketCap)
		valueArgs = append(valueArgs, "STK")
	}

	stmt := fmt.Sprintf("INSERT INTO STOCK( TICKER, DATE, NAME, MARKET, CHANGE_CODE, CHANGES, CHANGES_RATIO, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, VOLUME, AMOUNT, MARKET_CAP, MARKET_ID ) VALUES %s", strings.Join(valueStrings, ","))

	_, err = db.Exec(stmt, valueArgs...)

	if err != nil {
		fmt.Println(err)
		tx.Rollback()
		return
	}

	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("save")
}

func getChangeCode(change string) string {

	c, err := strconv.Atoi(change)

	if err != nil {
		return "0"
	}

	if c < 0 {
		return "2"
	} else if c > 0 {
		return "1"
	} else {
		return "3"
	}
}
