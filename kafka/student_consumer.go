package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"project/config"
	"project/logger"
	"project/model"
	"project/redis"
	"time"

	"github.com/IBM/sarama"
)

const (
	idempotentPrefix = "kafka:idempotent:"
	idempotentExpire = 24 * time.Hour
	maxRetryTimes    = 3
)

type StudentEventHandler struct {
	MaxRetry int
}

func (h *StudentEventHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}
func (h *StudentEventHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *StudentEventHandler) processEventWithRetry(event *model.StudentEvent) error {
	var err error
	for i := 0; i < maxRetryTimes; i++ {
		err = h.processEvent(event)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * time.Second)
		logger.Infof("事件第%d次重试:eventID=%s", i+1, event.EventID)
	}
	h.sendToDLQ(event)
	return fmt.Errorf("重试%d次全部失败，已转入死信队列", maxRetryTimes)
}
func (h *StudentEventHandler) sendToDLQ(event *model.StudentEvent) {
	dlqTopic := config.GlobalConfig.Kafka.TopicStudentEvent + "_dlq"
	eventJson, _ := json.Marshal(event)
	err := SendMessage(dlqTopic, event.EventID, string(eventJson))
	if err != nil {
		logger.Errorf("死信消息发送失败：%v", err)
	} else {
		logger.Warnf("消息已转入死信队列：eventID=%s", event.EventID)
	}
}
func (h *StudentEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event model.StudentEvent
		err := json.Unmarshal(msg.Value, &event)
		if err != nil {
			logger.Errorf("学生事件解析失败：%v，消息：%s", err, string(msg.Value))
			session.MarkMessage(msg, "")
			continue
		}
		c := context.Background()
		idempotentKey := fmt.Sprintf("%s%s", idempotentPrefix, event.EventID)
		exist, err := redis.Client.Exists(c, idempotentKey).Result()
		if err == nil && exist > 0 {
			logger.Infof("消息重复消费，跳过: %s", event.EventID)
			session.MarkMessage(msg, "")
			continue
		}
		err = h.processEventWithRetry(&event)
		if err != nil {
			logger.Errorf("事件处理失败:eventID=%s,err=%v", event.EventID, err)
			session.MarkMessage(msg, "")
			continue
		}
		redis.Client.Set(c, idempotentKey, "1", idempotentExpire)
		session.MarkMessage(msg, "")
	}
	return nil

}
func (h *StudentEventHandler) processEvent(event *model.StudentEvent) error {
	switch event.EventType {
	case "create":
		logger.Infof("收到学生创建事件：ID=%d", event.StudentID)
	case "update":
		logger.Infof("收到学生更新事件:ID=%d", event.StudentID)
	case "delete":
		logger.Infof("收到学生删除事件:ID=%d", event.StudentID)
	default:
		logger.Warnf("未知事件类型:%s", event.EventType)
	}
	return nil
}

func StartStudentConsumer(c context.Context) error {
	topics := []string{config.GlobalConfig.Kafka.TopicStudentEvent}
	return StartConsumer(c, topics, &StudentEventHandler{}, config.GlobalConfig.Kafka.GroupID+"-student")
}
