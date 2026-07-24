package service

import (
	"context"
	"encoding/json"
	"fmt"
	"project/dao"
	"project/kafka"
	"project/logger"
	"project/model"
	"time"

	"project/config"

	"gorm.io/gorm"
)

func StartLocalMessageSender(c context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	logger.Info("【本地消息发送器】启动成功，每5秒扫描一次")

	for {
		select {
		case <-c.Done():
			logger.Info("【本地消息发送器】收到停止信号，退出")
			return
		case <-ticker.C:
			SendPendingMessages()
		}
	}
}

func SendPendingMessages() {
	messages, err := dao.GetPendingMessages(100)
	if err != nil {
		logger.Errorf("【本地消息】查询待发送消息失败: %v", err)
		return
	}
	if len(messages) == 0 {
		return
	}
	logger.Infof("【本地消息】扫描到 %d 条待发送消息", len(messages))

	for _, msg := range messages {
		processOneMessage(msg)
	}
}

func processOneMessage(msg model.LocalMessage) {
	err := kafka.SendMessage(msg.Topic, msg.Key, msg.Value)
	if err != nil {
		handleSendFailure(msg, err)
		return
	}
	if err := dao.MarkMessageSent(msg.ID); err != nil {
		logger.Errorf("【本地消息】标记已发送失败: %v", err)
		return
	}
	logger.Infof("【本地消息】消息发送成功: id=%d, topic=%s", msg.ID, msg.Topic)
}

func handleSendFailure(msg model.LocalMessage, sendErr error) {
	newRetryCount := msg.RetryCount + 1

	if newRetryCount >= msg.MaxRetry {
		if err := dao.MarkMessageFailed(msg.ID, sendErr.Error()); err != nil {
			logger.Errorf("【本地消息】标记失败状态失败: %v", err)
		} else {
			logger.Errorf("【本地消息】消息已超过最大重试次数，转入失败队列: id=%d", msg.ID)
		}
		return
	}
	if err := dao.UpdateMessageStatus(msg.ID, 0, newRetryCount, sendErr.Error()); err != nil {
		logger.Errorf("【本地消息】更新重试状态失败: %v", err)
	} else {
		logger.Warnf("【本地消息】发送失败，将在 %d 秒后重试: id=%d, retry=%d/%d", int(5<<(newRetryCount-1)), msg.ID, newRetryCount, msg.MaxRetry)
	}
}

func SaveAuditMessage(tx *gorm.DB, auditMsg *model.AuditMessage) error {
	defer func() {
		auditMsg.TraceID = ""
		auditMsg.OperateType = 0
		auditMsg.Module = ""
		auditMsg.DataID = 0
		auditMsg.Operator = ""
		auditMsg.BeforeData = nil
		auditMsg.AfterData = nil
		auditMsg.IP = ""
		fmt.Printf("放回池子一个对象，地址: %p\n", auditMsg)
		auditMsgPool.Put(auditMsg)
	}()
	data, err := json.Marshal(auditMsg)
	if err != nil {
		return err
	}

	localMsg := &model.LocalMessage{
		Topic:      config.GlobalConfig.Kafka.TopicAuditLog,
		Key:        auditMsg.TraceID,
		Value:      string(data),
		Status:     0,
		RetryCount: 0,
		MaxRetry:   5,
		NextRetry:  time.Now(),
	}
	return dao.CreateLocalMessage(tx, localMsg)
}
