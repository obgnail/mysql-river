package kafka

//import (
//	"fmt"
//	"github.com/Shopify/sarama"
//	"github.com/obgnail/mysql-river/config"
//	"github.com/pingcap/errors"
//	"sync"
//)
//
//var producer sarama.SyncProducer
//
//var (
//	kafkaHost    = config.String("kafka_host", "127.0.0.1")
//	kafkaPort, _ = config.Int64("kafka_port", 9202)
//	kafkaAddr    = fmt.Sprintf("%s:%d", kafkaHost, kafkaPort)
//	kafkaTopic   = config.String("kafka_topic", "es_river")
//)
//
//func NewProducer(addrs []string) (sarama.SyncProducer, error) {
//	cfg := sarama.NewConfig()
//	cfg.Producer.RequiredAcks = sarama.WaitForAll
//	//cfg.Producer.Partitioner = sarama.NewRandomPartitioner
//	cfg.Producer.Return.Errors = true
//	cfg.Producer.Return.Successes = true
//
//	client, err := sarama.NewSyncProducer(addrs, cfg)
//	if err != nil {
//		return nil, errors.Trace(err)
//	}
//	return client, nil
//}
//
//func sendMessage(producer sarama.SyncProducer, topic string, content []byte) (partition int32, offset int64, err error) {
//	msg := &sarama.ProducerMessage{
//		Topic: topic,
//		Value: sarama.ByteEncoder(content),
//	}
//	partition, offset, err = producer.SendMessage(msg)
//	if err != nil {
//		err = errors.Trace(err)
//		return
//	}
//	return
//}
//
//func SendMessage(content []byte) (partition int32, offset int64, err error) {
//	return sendMessage(producer, kafkaTopic, content)
//}
//
//func Consum(f func(msg *sarama.ConsumerMessage) error) error {
//	return consum([]string{kafkaAddr}, kafkaTopic, f)
//}
//
//func consum(addrs []string, topic string, f func(msg *sarama.ConsumerMessage) error) error {
//	consumer, err := sarama.NewConsumer(addrs, nil)
//	if err != nil {
//		return errors.Trace(err)
//	}
//	partitionList, err := consumer.Partitions(topic)
//	if err != nil {
//		return errors.Trace(err)
//	}
//
//	var wg sync.WaitGroup
//	for p := range partitionList {
//		pc, err := consumer.ConsumePartition(topic, int32(p), sarama.OffsetNewest)
//		if err != nil {
//			return errors.Trace(err)
//		}
//		defer pc.AsyncClose()
//
//		wg.Add(1)
//		go func(sarama.PartitionConsumer) {
//			for msg := range pc.Messages() {
//				if err := f(msg); err != nil {
//					return
//				}
//			}
//		}(pc)
//	}
//	wg.Wait()
//	return nil
//}
//
//func init() {
//	var err error
//	producer, err = NewProducer([]string{kafkaAddr})
//	if err != nil {
//		panic(err)
//	}
//}
