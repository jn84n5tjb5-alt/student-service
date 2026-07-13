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
	go func() {
		data, err := json.Marshal(msg)
		if err != nil {
			logger.Errorf("序列化审计消息失败:%v", err)
			return
		}
		err = kafka.SendMessage(
			config.GlobalConfig.Kafka.TopicStudentEvent,
			msg.TraceID,
			string(data),
		)
		if err != nil {
			logger.Errorf("发送审计消息失败:%v,traceID=%s", err, msg.TraceID)
		}
	}()
}
