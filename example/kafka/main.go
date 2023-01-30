package main

import (
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/handler/kafka"
	"github.com/obgnail/mysql-river/river"
)

func main() {
	err := river.RunRiver("127.0.0.1:3306", "root", "root", kafka.NewKafkaHandler())
	errors.ErrorStack(errors.Trace(err))
}
