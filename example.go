package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"github.com/obgnail/mysql-river/river"
)

func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func Kafka() {
	r, err := river.New("127.0.0.1", 3306, "root", "root", "./", 0, 5, 3)
	PanicIfError(err)

	handler, err := kafka.New([]string{"127.0.0.1:9092"}, "binlog")
	PanicIfError(err)

	r.SetHandler(handler)

	go handler.Consume(func(msg *sarama.ConsumerMessage) error {
		fmt.Printf("Partition:%d, Offset:%d, key:%s, value:%s\n",
			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		return nil
	})

	err = r.RunFrom(river.FromInfoFile)
	PanicIfError(err)
}

func TraceLog() {
	r, err := river.New("127.0.0.1", 3306, "root", "root", "./", 0, 5, 3)
	PanicIfError(err)
	handler := trace_log.New([]string{"testdb01"}, false, true, true)
	r.SetHandler(handler)
	err = r.RunFrom(river.FromInfoFile)
	PanicIfError(err)
}

func Base() {
	r, err := river.New(
		"127.0.0.1", 3306, "root", "root",
		"./", 0,
		5, 3)
	PanicIfError(err)
	r.SetHandler(river.NopCloserAlerter(func(event *river.EventData) error {
		fmt.Println(event.EventType, event.LogName, event.LogPos, event.Before, event.After)
		return nil
	}))
	err = r.RunFrom(river.FromInfoFile)
	PanicIfError(err)
}

func main() {
	Base()
	//TraceLog()
	//Kafka()
}
