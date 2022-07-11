package config

import "github.com/obgnail/mysql-river/handler/es_old"

type MySQLConfig struct {
	MySQLHost     string `json:"mysql_host"`
	MySQLPort     int64  `json:"mysql_port"`
	MySQLUser     string `json:"mysql_user"`
	MySQLPassword string `json:"mysql_pass"`
}

type TraceLogHandlerConfig struct {
	Dbs              []string `json:"trace_log_dbs"`
	Tables           []string `json:"trace_log_tables"`
	ShowAllField     bool     `json:"trace_log_show_all_field"`
	ShowQueryMessage bool     `json:"trace_log_show_query_message"`
}

type KafkaHandlerConfig struct {
	KafkaHost  string `json:"kafka_host"`
	KafkaPort  int64  `json:"kafka_port"`
	KafkaTopic string `json:"kafka_topic"`
}

type RiverConfig struct {
	Mysql           *MySQLConfig
	KafkaHandler    *KafkaHandlerConfig
	TraceLogHandler *TraceLogHandlerConfig
	ESHandler       *es_old.SyncESConfig
}


