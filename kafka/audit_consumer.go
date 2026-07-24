package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"project/config"
	"project/dao"
	"project/logger"
	"project/model"
	"project/redis"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"gorm.io/gorm"
)

const auditIdempotentKey = "audit:consumed:%s"

type AuditLogHandler struct {
}

func (h *AuditLogHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil

}

func (h *AuditLogHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
func (h *AuditLogHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logger.Info("【审计消费者】ConsumeClaim 开始运行") // 新增
	for msg := range claim.Messages() {
		logger.Infof("【审计消费者】收到消息: %s", string(msg.Value)) // 新增
		var auditMsg model.AuditMessage
		if err := json.Unmarshal(msg.Value, &auditMsg); err != nil {
			logger.Errorf("解析审计消息失败：%v,原始:%s", err, string(msg.Value))
			sess.MarkMessage(msg, "")
			continue
		}
		if err := ConsumeAuditLog(&auditMsg); err != nil {
			logger.Errorf("审计消息落库失败:%v,traceID=%s", err, auditMsg.TraceID)
			continue
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func StartAuditConsumer(c context.Context) error {
	topics := []string{config.GlobalConfig.Kafka.TopicAuditLog}
	return StartConsumer(c, topics, &AuditLogHandler{}, config.GlobalConfig.Kafka.GroupID+"-audit")
}

func ConsumeAuditLog(msg *model.AuditMessage) error {
	key := fmt.Sprintf(auditIdempotentKey, msg.TraceID)
	exist, err := redis.Client.SetNX(context.Background(), key, 1, 24*time.Hour).Result()
	if err != nil {
		logger.Errorf("【审计落库】Redis SetNX 失败: %v, traceID=%s", err, msg.TraceID)

		return err
	}
	if !exist {
		logger.Infof("【审计落库】重复消息，跳过 traceID=%s", msg.TraceID)
		return nil
	}
	beforeJSON, _ := json.Marshal(msg.BeforeData)
	afterJSON, _ := json.Marshal(msg.AfterData)

	auditLog := &model.AuditLog{
		TraceID:     msg.TraceID,
		OperateType: msg.OperateType,
		Module:      msg.Module,
		DataID:      msg.DataID,
		Operator:    msg.Operator,
		BeforeData:  string(beforeJSON),
		AfterData:   string(afterJSON),
		IP:          msg.IP,
	}
	err = dao.CreateAuditLog(auditLog)
	if err != nil {
		if isDuplicateKeyError(err) {
			return nil
		}
		return err
	}
	logger.Infof("【审计落库】成功写入审计日志: traceID=%s, module=%s, dataID=%d",
		msg.TraceID, msg.Module, msg.DataID)
	return nil
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, gorm.ErrDuplicatedKey) ||
		strings.Contains(err.Error(), "Error 1062")
}
