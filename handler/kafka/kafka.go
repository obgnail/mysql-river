package kafka

import (
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/obgnail/mysql-river/river"
	"os"
	"path"
)

var offsetStoreName = "kafka_offset.bolt"

type Config struct {
	Addrs           []string `json:"addrs"`
	Topic           string   `json:"topic"`
	OffsetStoreDir  string   `json:"offset_store_dir"`
	Offset          *int64   `json:"offsetStore"` // if it has no offset, set nil
	UseOldestOffset bool     `json:"use_oldest_offset"`
}

func (c *Config) GetOffset() int64 {
	if c.UseOldestOffset {
		return sarama.OffsetOldest
	}
	if c.Offset == nil {
		return sarama.OffsetNewest
	}
	return *c.Offset
}

// Broker example:
//		handler, err := New(brokerConfig)
//		handler.SetEventMarshaller(...)
//		go handler.Consume(func(msg *sarama.ConsumerMessage) error {
//			// consume your event
//		})
//		go river.New(riverConfig).SetEventMarshaller(handler).Sync(river.FromFile)
type Broker struct {
	config *Config

	offsetStore *Offset

	eventMarshaller func(event *river.EventData) ([]byte, error)

	producer sarama.SyncProducer

	river.NopCloserAlerter
}

var _ river.Handler = (*Broker)(nil)

func New(config *Config) (*Broker, error) {
	if len(config.OffsetStoreDir) == 0 {
		return nil, fmt.Errorf("offsetStore store dir is empty")
	}
	if err := os.MkdirAll(config.OffsetStoreDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}
	filePath := path.Join(config.OffsetStoreDir, offsetStoreName)
	offset, err := NewOffset(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	producer, err := NewProducer(config.Addrs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	h := &Broker{
		config:          config,
		offsetStore:     offset,
		producer:        producer,
		eventMarshaller: river.Event2Bytes,
	}
	return h, nil
}

func (b *Broker) SetEventMarshaller(eventMarshaller func(event *river.EventData) ([]byte, error)) *Broker {
	b.eventMarshaller = eventMarshaller
	return b
}

func (b *Broker) String() string {
	return "kafka"
}

func (b *Broker) OnEvent(event *river.EventData) error {
	result, err := b.eventMarshaller(event)
	if err != nil {
		return errors.Trace(err)
	}
	if len(result) == 0 {
		return nil
	}
	if _, _, err = SendMessage(b.producer, b.config.Topic, result); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (b *Broker) useStoredOffsetIfExists(partition int32, offset int64) (int64, error) {
	offsetByte, err := b.offsetStore.Get(b.config.Topic, partition)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(offsetByte) > 0 {
		offset = int64(binary.LittleEndian.Uint64(offsetByte))
	}
	river.Logger.Debugf("load partition %d offset: %d", partition, offset)
	return offset, nil
}

func (b *Broker) Consume(f func(msg *sarama.ConsumerMessage) error) error {
	offsetGetter := func(partition int32) (offset int64, err error) {
		offset = b.config.GetOffset()
		offset, err = b.useStoredOffsetIfExists(partition, offset)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return offset, nil
	}

	consumer := func(msg *sarama.ConsumerMessage) error {
		if err := f(msg); err != nil {
			return errors.Trace(err)
		}
		if err := b.offsetStore.Put(msg.Topic, msg.Partition, msg.Offset); err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	return Consume(b.config.Addrs, b.config.Topic, offsetGetter, consumer)
}
