package kafka

import (
	"context"
	"fmt"
	"project/config"
	"time"

	"project/logger"

	"github.com/IBM/sarama"
)

var Producer sarama.SyncProducer

func InitKafka() error {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	switch config.GlobalConfig.Kafka.AckMode {
	case "wait_for_all":
		cfg.Producer.RequiredAcks = sarama.WaitForAll
	case "wait_for_local":
		cfg.Producer.RequiredAcks = sarama.WaitForLocal
	default:
		cfg.Producer.RequiredAcks = sarama.WaitForAll
	}
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Retry.Backoff = 100 * time.Millisecond

	var err error
	Producer, err = sarama.NewSyncProducer(config.GlobalConfig.Kafka.Brokers, cfg)
	if err != nil {
		return fmt.Errorf("kafka生产者初始化失败:%w", err)
	}
	logger.Info("kafka生产者初始化成功")
	return nil
}

func SendMessage(topic string, key string, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	_, _, err := Producer.SendMessage(msg)
	return err
}

func StartConsumer(c context.Context, handler sarama.ConsumerGroupHandler) error {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup(
		config.GlobalConfig.Kafka.Brokers,
		config.GlobalConfig.Kafka.GroupID,
		cfg,
	)
	if err != nil {
		return fmt.Errorf("kafka消费者组创建失败:%w", err)
	}
	go func() {
		for {
			select {
			case <-c.Done():
				logger.Info("kafka消费者停止")
				return
			default:
				topics := []string{config.GlobalConfig.Kafka.TopicStudentEvent}
				err := group.Consume(c, topics, handler)
				if err != nil {
					logger.Errorf("kafka消费者异常:%v", err)
				}
			}
		}
	}()
	logger.Info("kafka消费者启动成功")
	return nil
}
