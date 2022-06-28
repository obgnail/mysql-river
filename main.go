package main

import (
	"flag"
	"fmt"
	"github.com/juju/errors"
	"log"
	"strings"
)

var (
	host     string
	port     int64
	user     string
	password string
	branch   string
	tables   string
	more     bool
	tx       bool

	databases string

	databasesList  []string
	onlyTablesList []string
)

const asciiArt = `
 __    __     __  __     ______     ______     __         ______     __     __   __   ______     ______    
/\ "-./  \   /\ \_\ \   /\  ___\   /\  __ \   /\ \       /\  == \   /\ \   /\ \ / /  /\  ___\   /\  == \   
\ \ \-./\ \  \ \____ \  \ \___  \  \ \ \/\_\  \ \ \____  \ \  __<   \ \ \  \ \ \'/   \ \  __\   \ \  __<   
 \ \_\ \ \_\  \/\_____\  \/\_____\  \ \___\_\  \ \_____\  \ \_\ \_\  \ \_\  \ \__|    \ \_____\  \ \_\ \_\ 
  \/_/  \/_/   \/_____/   \/_____/   \/___/_/   \/_____/   \/_/ /_/   \/_/   \/_/      \/_____/   \/_/ /_/ 
                                                                                                          
`

func init() {
	flag.StringVar(&host, "h", "127.0.0.1", "mysql host")
	flag.Int64Var(&port, "P", 3306, "mysql port")
	flag.StringVar(&user, "u", "onesdev", "mysql user")
	flag.StringVar(&password, "p", "onesdev", "mysql user password")
	flag.StringVar(&branch, "b", "master", "branch you want, if d arg is set, be invalidated")
	flag.StringVar(&databases, "d", "", "dbs you want, split by comma")
	flag.StringVar(&tables, "t", "", "tables you want, split by comma")
	flag.BoolVar(&more, "v", false, "show more message in update sql")
	flag.BoolVar(&tx, "tx", false, "show DDL/XID/GTID msg")
	flag.Parse()

	databasesList = []string{
		fmt.Sprintf("project_%s", strings.ToLower(branch)),
		fmt.Sprintf("wiki_%s", strings.ToLower(branch)),
	}
	if databases != "" {
		databasesList = databasesList[:0]
		for _, v := range strings.Split(databases, ",") {
			v = strings.ToLower(strings.TrimSpace(v))
			databasesList = append(databasesList, v)
		}
	}

	if tables != "" {
		for _, v := range strings.Split(tables, ",") {
			v = strings.ToLower(strings.TrimSpace(v))
			onlyTablesList = append(onlyTablesList, v)
		}
	}

	fmt.Println("---- args ----")
	fmt.Println("host:\t", host)
	fmt.Println("port:\t", port)
	fmt.Println("user:\t", user)
	fmt.Println("pw:\t", password)
	fmt.Println("v:\t", more)
	fmt.Println("tx:\t", tx)
	fmt.Printf("dbs:\t%+v\n", databasesList)
	fmt.Printf("tables:\t%+v\n", onlyTablesList)
	fmt.Println("--------------")
	fmt.Println(asciiArt)
}

func main() {
	uri := fmt.Sprintf("%s:%d", host, port)
	r := NewRiver(uri, user, password, databasesList, onlyTablesList, more, tx)
	if err := r.Listen(); err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
