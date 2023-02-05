package elasticsearch

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/river"
	"time"
)

const (
	minFlushInterval     = time.Second
	defaultFlushInterval = 3 * time.Second

	defaultBulkSize = 128
)

type ESHandler struct {
	config *EsHandlerConfig

	rules map[string]map[string]*Rule // map[dbName]map[tableName]*Rule

	esClient *Client

	sendChan        chan *BulkRequest
	stopHandlerChan chan struct{} // when river throws error, river will put one int, to stop handler
	stopRiverChan   chan struct{} // when handler throws error, handler will put on int, to stop river
}

var _ river.Handler = (*ESHandler)(nil)

func New(config *EsHandlerConfig) *ESHandler {
	h := &ESHandler{config: config}
	h.prepare()
	go h.SyncLoop()
	return h
}

func (h *ESHandler) String() string {
	return "elasticsearch"
}

func (h *ESHandler) prepare() {
	if h.config.BulkSize <= 0 {
		h.config.BulkSize = defaultBulkSize
	}
	if h.config.FlushInterval < minFlushInterval {
		h.config.FlushInterval = defaultFlushInterval
	}
	h.esClient = NewClient(&ClientConfig{
		Addr:     fmt.Sprintf("%s:%d", h.config.Host, h.config.Port),
		User:     h.config.User,
		Password: h.config.Password,
	})
	h.rules = h.prepareRule()
	h.stopHandlerChan = make(chan struct{}, 1)
	h.stopRiverChan = make(chan struct{}, 1)
	h.sendChan = make(chan *BulkRequest, h.config.BulkSize)
}

func (h *ESHandler) prepareRule() map[string]map[string]*Rule {
	if !h.config.SkipNoPkTable {
		for _, rule := range h.config.Rules {
			if len(rule.ID) == 0 {
				panic("existing rule which ID is none, while skipNoPkTable is set false")
			}
		}
	}

	rules := make(map[string]map[string]*Rule)
	for _, rule := range h.config.Rules {
		rules[rule.Schema] = map[string]*Rule{rule.Table: rule}
	}
	return rules
}

func (h *ESHandler) pickRule(event *river.EventData) *Rule {
	_, ok := h.rules[event.Db]
	if !ok {
		return nil
	}
	rule, ok := h.rules[event.Db][event.Table]
	if !ok {
		return nil
	}
	return rule
}

func (h *ESHandler) OnClose(r *river.River) {
	river.Logger.Error(1111111111, r.Error)
	h.stopHandlerChan <- struct{}{}
}

func (h *ESHandler) OnAlert(msg *river.StatusMsg) error {
	return nil
}

func (h *ESHandler) OnEvent(event *river.EventData) error {
	if len(h.stopRiverChan) != 0 {
		return errors.New("es handler actively throws stop error")
	}
	switch event.EventType {
	case river.EventTypeTableChanged:
		h.WhenTableChanged(event)
	case river.EventTypeInsert, river.EventTypeDelete, river.EventTypeUpdate:
		for _, req := range h.Convert(event) {
			h.sendChan <- req
		}
	}
	return nil
}

func (h *ESHandler) WhenTableChanged(event *river.EventData) {
	return
}

func (h *ESHandler) Convert(event *river.EventData) (reqs []*BulkRequest) {
	if h.config.SkipNoPkTable && len(event.Primary) == 0 {
		return
	}
	rule := h.pickRule(event)
	if rule == nil {
		return
	}
	reqs = rule.ToReqs(event)
	return reqs
}

func (h *ESHandler) SyncLoop() {
	ticker := time.NewTicker(h.config.FlushInterval)
	defer ticker.Stop()

	bulk := make([]*BulkRequest, 0, h.config.BulkSize)
	for {
		needFlush := false
		select {
		case <-ticker.C:
			needFlush = true
		case <-h.stopHandlerChan:
			return
		case req := <-h.sendChan:
			bulk = append(bulk, req)
			needFlush = len(bulk) >= h.config.BulkSize
		}

		if needFlush {
			if err := h.sync(bulk); err != nil {
				// 一旦同步异常,直接停止同步
				h.stopRiverChan <- struct{}{}
			}
			bulk = bulk[0:0]
		}
	}
}

func (h *ESHandler) sync(reqs []*BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	if err := h.doBulk(reqs); err != nil {
		return errors.Trace(err)
	}
	reqs = reqs[0:0]
	return nil
}

func (h *ESHandler) doBulk(reqs []*BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	resp, err := h.esClient.Bulk(reqs)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Code/100 == 2 || resp.Errors {
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				if len(item.Error) > 0 {
					return fmt.Errorf("%s index: %s, type: %s, id: %s, status: %d, error: %s",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
				}
			}
		}
	}
	return nil
}
