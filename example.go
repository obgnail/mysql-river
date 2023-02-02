package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"github.com/obgnail/mysql-river/river"
)

//
//func TraceLog() {
//	canal, _ := river.NewCanalFromConfig()
//	handler := trace_log.NewFromConfig()
//	go handler.Consume(func(sql string) { fmt.Println(sql) })
//	river.RunFrom(canal, handler)
//}
//
//func TraceLog2() {
//	canal, _ := river.NewCanal("127.0.0.1", 3306, "root", "root")
//	handler := trace_log.New([]string{"testdb01"}, false, false, true)
//	go handler.Consume(func(sql string) { fmt.Println(sql) })
//	river.RunFrom(canal, handler)
//}
//
//func Kafka() {
//	canal, _ := river.NewCanalFromConfig()
//	handler, _ := kafka.NewFromConfig()
//	go handler.Consume(func(msg *sarama.ConsumerMessage) error {
//		fmt.Printf("Partition:%d, Offset:%d, key:%s, value:%s\n",
//			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
//		return nil
//	})
//	river.RunFrom(canal, handler)
//}
//
//func Kafka2() {
//	canal, _ := river.NewCanal("127.0.0.1", 3306, "root", "root")
//	handler, _ := kafka.New([]string{"127.0.0.1:9092"}, "binlog")
//	go handler.Consume(func(msg *sarama.ConsumerMessage) error {
//		fmt.Printf("Partition:%d, Offset:%d, key:%s, value:%s\n",
//			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
//		return nil
//	})
//	river.RunFrom(canal, handler)
//}

func PanicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func Kafka() {
	r, err := river.New("127.0.0.1", 3306, "root", "root", "./")
	PanicIfError(err)

	handler, err := kafka.New([]string{"127.0.0.1:9092"}, "binlog")
	PanicIfError(err)

	r.SetHandlerFunc(handler.Send)

	go handler.Consume(func(msg *sarama.ConsumerMessage) error {
		fmt.Printf("Partition:%d, Offset:%d, key:%s, value:%s\n",
			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		return nil
	})

	err = r.RunFrom(river.FromInfoFile)
	PanicIfError(err)
}

func TraceLog() {
	r, err := river.New("127.0.0.1", 3306, "root", "root", "./")
	PanicIfError(err)
	handler := trace_log.New([]string{"testdb01"}, true, true, false)
	r.SetHandlerFunc(handler.Show)
	err = r.RunFrom(river.FromMasterPos)
	PanicIfError(err)
}

func Base() {
	r, err := river.New("127.0.0.1", 3306, "root", "root", "./")
	PanicIfError(err)
	r.SetHandlerFunc(func(event *river.EventData) error {
		fmt.Println(event.EventType, event.LogName, event.LogPos, event.Before, event.After)
		return nil
	})
	err = r.RunFrom(river.FromInfoFile)
	PanicIfError(err)
}

func main() {
	//Base()
	TraceLog()
	//Kafka()
}
