package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/obgnail/mysql-river/handler/elasticsearch"
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
	PosAutoSaverConfig: &river.PosAutoSaverConfig{
		SaveDir:      "./",
		SaveInterval: 3 * time.Second,
	},
	HealthCheckerConfig: &river.HealthCheckerConfig{
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
	kafkaConfig := &kafka.Config{
		Addrs:           []string{"127.0.0.1:9092"},
		Topic:           "binlog",
		OffsetStoreDir:  "./",
		Offset:          nil,
		UseOldestOffset: false,
	}
	handler, err := kafka.New(kafkaConfig)
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
	dbs := []string{"testdb01"}
	entireFields := false // show all field message in update sql
	showTxMsg := true     // show transition msg in sql
	highlight := true     //  highlight sql expression
	handler := trace_log.New(dbs, entireFields, showTxMsg, highlight)
	err := river.New(config).SetHandler(handler).Sync(river.FromDB) // 从最新位置开始解析
	PanicIfError(err)
}

func ElasticSearch() {
	handlerConfig := &elasticsearch.EsHandlerConfig{
		Host:          "127.0.0.1",
		Port:          9200,
		User:          "",
		Password:      "",
		BulkSize:      128,
		FlushInterval: time.Second,
		SkipNoPkTable: true,
		Rules: []*elasticsearch.Rule{
			elasticsearch.NewDefaultRule("testdb01", "user"),
		},
	}
	handler := elasticsearch.New(handlerConfig)
	err := river.New(config).SetHandler(handler).Sync(river.FromDB) // 从最新位置开始解析
	PanicIfError(err)
}

func Base() {
	err := river.New(config).
		SetHandler(river.NopCloserAlerter(func(event *river.EventData) error {
			fmt.Println(event.EventType, event.LogName, event.LogPos, event.Before, event.After)
			return nil
		})).
		Sync(river.FromDB)
	PanicIfError(err)
}

func main() {
	//Base()
	//TraceLog()
	Kafka()
	//ElasticSearch()
}
