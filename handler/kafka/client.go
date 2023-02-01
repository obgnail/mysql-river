package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"sync"
)

func NewProducer(addrs []string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = true

	client, err := sarama.NewSyncProducer(addrs, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client, nil
}

func SendMessage(producer sarama.SyncProducer, topic string, content []byte) (partition int32, offset int64, err error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(content),
	}
	partition, offset, err = producer.SendMessage(msg)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

func Consume(addrs []string, topic string, f func(msg *sarama.ConsumerMessage) error) error {
	consumer, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		return errors.Trace(err)
	}
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		return errors.Trace(err)
	}
	var wg sync.WaitGroup
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			return errors.Trace(err)
		}
		defer pc.AsyncClose()
		wg.Add(1)
		go func(sarama.PartitionConsumer) { // 为每个分区开一个go协程去取值
			for msg := range pc.Messages() { // 阻塞直到有值发送过来，然后再继续等待
				err = f(msg)
				errors.ErrorStack(errors.Trace(err))
			}
		}(pc)
	}
	wg.Wait()
	return nil
}
