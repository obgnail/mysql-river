package kafka

import (
	"encoding/binary"
	"fmt"
	"github.com/juju/errors"
)

type Offset struct {
	store *store
}

var kafkaOffsetBucket = []byte("kafka")

func NewOffset(storePath string) (*Offset, error) {
	s, err := newStore(storePath, kafkaOffsetBucket)
	if err != nil {
		return nil, errors.Trace(err)
	}
	offset := &Offset{
		store: s,
	}
	return offset, nil
}

func (o *Offset) Get(topic string, partition int32) ([]byte, error) {
	key := buildKey(topic, partition)
	offsetByte, err := o.store.Get(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return offsetByte, nil
}

func (o *Offset) Put(topic string, partition int32, offset int64) error {
	raw := make([]byte, 8)
	v := uint64(offset)
	binary.LittleEndian.PutUint64(raw, v)
	key := buildKey(topic, partition)
	err := o.store.Put(key, raw)
	return errors.Trace(err)
}

func buildKey(topic string, partition int32) []byte {
	return []byte(fmt.Sprintf("%s-%d", topic, partition))
}
