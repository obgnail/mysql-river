package elasticsearch

import "time"

type EsHandlerConfig struct {
	Host          string
	Port          int64
	User          string
	Password      string
	BulkSize      int
	FlushInterval time.Duration
	SkipNoPkTable bool
	Rules         []*Rule
}
