package main

import (
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/handler/trace_log"
)

func TraceLog() {
	err := trace_log.RunTraceLogRiver("127.0.0.1:3306", "root", "root",
		[]string{"testdb01"}, false, false)
	errors.ErrorStack(errors.Trace(err))
}

func Kafka() {
	err := kafka.RunKafkaRiver("127.0.0.1:3306", "root", "root")
	errors.ErrorStack(errors.Trace(err))
}

func main() {
	Kafka()
}
