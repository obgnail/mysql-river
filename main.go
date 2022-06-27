package main

import (
	"github.com/juju/errors"
	"log"
)

func main() {
	r := NewRiver("127.0.0.1:3306", "root", "root")
	if err := r.Listen(); err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
