package kafka

import (
	"context"
	"fmt"
	"os"
	"project/config"
	"strings"
	"time"

	"project/logger"

	"github.com/IBM/sarama"
	"github.com/sony/gobreaker"
)

var Producer sarama.SyncProducer
var kafkaBreaker *gobreaker.CircuitBreaker

func InitBreaker() {
	var st gobreaker.Settings
	st.Name = "KafkaBreaker"
	st.MaxRequests = 1
	st.Timeout = 5 * time.Second
	st.ReadyToTrip = func(counts gobreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 3 && failureRatio >= 0.5
	}
	kafkaBreaker = gobreaker.NewCircuitBreaker(st)
}

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

	logger.Infof("【Kafka调试】Producer 使用的 Brokers: %v", config.GlobalConfig.Kafka.Brokers)
	var err error
	Producer, err = sarama.NewSyncProducer(config.GlobalConfig.Kafka.Brokers, cfg)
	if err != nil {
		return fmt.Errorf("kafka生产者初始化失败:%w", err)
	}
	logger.Info("kafka生产者初始化成功")
	InitBreaker()
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
func SendMessageWithTimeout(topic string, key string, value string, timeout time.Duration) error {
	_, err := kafkaBreaker.Execute(func() (interface{}, error) {
		done := make(chan error, 1)
		go func() {
			done <- SendMessage(topic, key, value)
		}()
		select {
		case err := <-done:
			return nil, err
		case <-time.After(timeout):
			return nil, fmt.Errorf("发送kafka消息超时:%v", timeout)
		}
	})
	if err != nil {
		if err == gobreaker.ErrOpenState {
			logger.Warn("【熔断器】kafka熔断器已开启，快速失败")
		}
		return err
	}
	return nil
}

func StartConsumer(c context.Context, topics []string, handler sarama.ConsumerGroupHandler, groupID string) error {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := config.GlobalConfig.Kafka.Brokers
	if len(brokers) == 0 || brokers[0] == "" {
		if env := os.Getenv("APP_KAFKA_BROKERS"); env != "" {
			brokers = strings.Split(env, ",")
		}
	}
	// ========================================

	// 加上调试日志
	logger.Infof("【Kafka调试】Consumer 最终使用的 Brokers: %v", brokers)
	group, err := sarama.NewConsumerGroup(
		brokers,
		groupID,
		cfg,
	)
	logger.Infof("【Kafka调试】Producer 使用的 Brokers: %v", config.GlobalConfig.Kafka.Brokers)
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
				logger.Infof("【调试】开始调用 group.Consume, topics=%v", topics)
				err := group.Consume(c, topics, handler)
				logger.Infof("【调试】group.Consume 返回, err=%v", err) // 新增
				if err != nil {
					logger.Errorf("kafka消费者异常:%v", err)
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()
	logger.Info("kafka消费者启动成功,groupID=%s", groupID)
	return nil
}
