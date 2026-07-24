package service

import (
	"encoding/json"
	"project/config"
	"project/kafka"
	"project/logger"
	"project/model"
)

// 发送审计消息
func SendAuditMessage(msg *model.AuditMessage) {
	logger.Infof("【审计】SendAuditMessage 被调用, traceID=%s", msg.TraceID)
	go func() {
		logger.Infof("【审计】开始序列化消息, traceID=%s", msg.TraceID)
		data, err := json.Marshal(msg)

		if err != nil {
			logger.Errorf("序列化审计消息失败:%v", err)
			return
		}
		logger.Infof("【审计】序列化成功, 即将发送到 topic=%s", config.GlobalConfig.Kafka.TopicAuditLog)
		err = kafka.SendMessage(
			config.GlobalConfig.Kafka.TopicAuditLog,
			msg.TraceID,
			string(data),
		)
		if err != nil {
			logger.Errorf("发送审计消息失败:%v,traceID=%s", err, msg.TraceID)
		} else {
			logger.Infof("【审计】消息发送成功, traceID=%s", msg.TraceID) // 新增
		}
	}()
}
