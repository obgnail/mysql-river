package main

import (
	"fmt"
	"github.com/obgnail/mysql-river/config"
	"github.com/obgnail/mysql-river/handler/es_old"
	"github.com/obgnail/mysql-river/river"
)

func main() {
	host := config.String("mysql_host", "127.0.0.1")
	port, _ := config.Int64("mysql_port", 3306)
	user := config.String("mysql_user", "root")
	password := config.String("mysql_pass", "root")
	addr := fmt.Sprintf("%s:%d", host, port)

	canal, err := river.NewRiver(addr, user, password)
	if err != nil {
		panic(err)
	}
	handler, err := es_old.NewSyncESHandler(config.Config.ESHandler, canal)
	if err != nil {
		panic(err)
	}
	canal.SetEventHandler(handler)
	if err := canal.RunFrom(handler.Position()); err != nil {
		panic(err)
	}
}
