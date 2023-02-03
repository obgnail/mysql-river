package my_es

import (
	"github.com/obgnail/mysql-river/river"
	"time"
)

type ESHandler struct {
	bulk     int
	interval time.Duration
	sendChan chan *BulkRequest
	exitChan chan struct{}
}

func (h *ESHandler) Loop() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	bulk := make([]*BulkRequest, 0, 1024)
	for {
		needFlush := false
		select {
		case <-ticker.C:
			needFlush = true
		case <-h.exitChan:
			return
		case req := <-h.sendChan:
			bulk = append(bulk, req)
			needFlush = len(bulk) >= h.bulk
		}

		if needFlush {
			if err := h.sync(bulk); err != nil {
				h.exitChan <- struct{}{}
			}
			bulk = bulk[0:0]
		}
	}
}

func (h *ESHandler) sync(reqs []*BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	return nil
}

func (h *ESHandler) Convert(event *river.EventData) []*BulkRequest {
	return nil
}

func (h *ESHandler) Handle(event *river.EventData) error {
	switch event.EventType {
	case river.EventTypeInsert, river.EventTypeDelete, river.EventTypeUpdate:
		for _, req := range h.Convert(event) {
			h.sendChan <- req
		}
	}
	return nil
}
