package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/river"
)

type KafkaHandler struct {
	topic string
	addrs []string

	producer sarama.SyncProducer

	river.NopCloserAlerter
}

var _ river.Handler = (*KafkaHandler)(nil)

func New(addrs []string, topic string) (*KafkaHandler, error) {
	producer, err := NewProducer(addrs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &KafkaHandler{
		addrs:    addrs,
		topic:    topic,
		producer: producer,
	}, nil
}

func (h *KafkaHandler) String() string {
	return "kafka"
}

func (h *KafkaHandler) OnEvent(event *river.EventData) error {
	result, err := event.ToBytes()
	if err != nil {
		return errors.Trace(err)
	}
	if _, _, err = SendMessage(h.producer, h.topic, result); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (h *KafkaHandler) Consume(f func(msg *sarama.ConsumerMessage) error) error {
	return Consume(h.addrs, h.topic, f)
}
