package river

import "time"

type MySQLConfig struct {
	Host     string
	Port     int64
	User     string
	Password string
}

type PosAutoSaver struct {
	SaveDir      string
	SaveInterval time.Duration
}

type HealthChecker struct {
	CheckInterval     time.Duration
	CheckPosThreshold int
}

type Config struct {
	*MySQLConfig
	*PosAutoSaver
	*HealthChecker
}

type From string

const (
	FromDB   From = "db-position"
	FromFile From = "file-position"
)
