package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	for msg := range claim.Messages() {
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
	return StartConsumer(c, &AuditLogHandler{})
}

func ConsumeAuditLog(msg *model.AuditMessage) error {
	key := fmt.Sprintf(auditIdempotentKey, msg.TraceID)
	exist, err := redis.Client.SetNX(context.Background(), key, 1, 24*time.Hour).Result()
	if err != nil {
		return err
	}
	if !exist {
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
	return nil
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, gorm.ErrDuplicatedKey) ||
		strings.Contains(err.Error(), "Error 1062")
}
