package main

import (
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/handler/trace_log"
	"github.com/obgnail/mysql-river/river"
)

func main() {
	handler := trace_log.NewTraceLogHandler(
		[]string{"testdb01"}, false, false)
	err := river.RunRiver("127.0.0.1:3306", "root", "root", handler)
	errors.ErrorStack(errors.Trace(err))
}
