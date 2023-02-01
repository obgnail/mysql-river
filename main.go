package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"github.com/obgnail/mysql-river/river"
)

func TraceLog() {
	canal, _ := river.NewCanalFromConfig()
	handler := trace_log.NewFromConfig()
	go handler.Consume(func(sql string) {
		fmt.Println(sql)
	})
	river.Run(canal, handler)
}

func Kafka() {
	canal, _ := river.NewCanalFromConfig()
	handler, _ := kafka.NewFromConfig()
	go handler.Consume(func(msg *sarama.ConsumerMessage) error {
		fmt.Printf("Partition:%d, Offset:%d, key:%s, value:%s\n",
			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		return nil
	})
	river.Run(canal, handler)
}

func main() {
	//TraceLog()
	Kafka()
}
