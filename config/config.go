package config

import (
	"github.com/BurntSushi/toml"
)

var Config RiverConfig

type RiverConfig struct {
	Mysql    *MySQL
	TraceLog *TraceLog `toml:"trace_log"`
	Kafka    *Kafka
	//ESHandler   *es_old.SyncESConfig `toml:"es"`
}

type MySQL struct {
	Host     string
	Port     int64
	User     string
	Password string `toml:"pass"`
}

type TraceLog struct {
	Dbs              []string
	ShowAllField     bool
	ShowQueryMessage bool
	Highlight        bool
}

type Kafka struct {
	Addrs []string
	Topic string
}

func init() {
	if _, err := toml.DecodeFile("config/config.toml", &Config); err != nil {
		panic(err)
	}
}
