package river

import "time"

type MySQLConfig struct {
	Host     string
	Port     int64
	User     string
	Password string
}

type PosAutoSaverConfig struct {
	SaveDir      string
	SaveInterval time.Duration
}

type HealthCheckerConfig struct {
	CheckInterval     time.Duration
	CheckPosThreshold int
}

type Config struct {
	*MySQLConfig
	*PosAutoSaverConfig
	*HealthCheckerConfig
}

type From string

const (
	FromDB   From = "db-position"
	FromFile From = "file-position"
)
