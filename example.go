package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"github.com/obgnail/mysql-river/river"
	"time"
)

var config = &river.Config{
	MySQLConfig: &river.MySQLConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "root",
	},
	PosAutoSaver: &river.PosAutoSaver{
		SaveDir:      "./",
		SaveInterval: 3 * time.Second,
	},
	HealthChecker: &river.HealthChecker{
		CheckPosThreshold: 3000,
		CheckInterval:     5 * time.Second,
	},
}

func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func Kafka() {
	handler, err := kafka.New([]string{"127.0.0.1:9092"}, "binlog")
	PanicIfError(err)
	go handler.Consume(func(msg *sarama.ConsumerMessage) error {
		fmt.Printf("Partition:%d, Offset:%d, key:%s, value:%s\n",
			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		return nil
	})
	err = river.New(config).SetHandler(handler).Sync(river.FromFile)
	PanicIfError(err)
}

func TraceLog() {
	handler := trace_log.New([]string{"testdb01"}, false, true, true)
	err := river.New(config).SetHandler(handler).Sync(river.FromFile)
	PanicIfError(err)
}

func Base() {
	err := river.New(config).
		SetHandler(river.NopCloserAlerter(func(event *river.EventData) error {
			fmt.Println(event.EventType, event.LogName, event.LogPos, event.Before, event.After)
			return nil
		})).
		Sync(river.FromFile)
	PanicIfError(err)
}

func main() {
	//Base()
	TraceLog()
	//Kafka()
}
