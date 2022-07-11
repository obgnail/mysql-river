package es

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

type Config struct {
	MyAddr     string `toml:"my_addr"`
	MyUser     string `toml:"my_user"`
	MyPassword string `toml:"my_pass"`
	MyCharset  string `toml:"my_charset"`

	ESAddr string `toml:"es_addr"`

	StatAddr string `toml:"stat_addr"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	DumpExec string `toml:"mysqldump"`

	Sources []SourceConfig `toml:"source"`

	Rules []*Rule `toml:"rule"`

	HealthCheckInterval     int    `toml:"health_check_interval"`
	HealthCheckPosThreshold int    `toml:"health_check_pos_threshold"`
	HealthCheckEnable       bool   `toml:"health_check_enable"`
	HealthCheckOuputDir     string `toml:"health_check_output_dir"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if c.MyCharset == "" {
		c.MyCharset = "utf8mb4"
	}

	return &c, nil
}

