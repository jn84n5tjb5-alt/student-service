package model

import "time"

type LocalMessage struct {
	ID         uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	Topic      string    `gorm:"type:varchar(64);not null;index" json:"topic"` // Kafka 主题
	Key        string    `gorm:"type:varchar(64)" json:"key"`                  // 消息 Key
	Value      string    `gorm:"type:text;not null" json:"value"`              // 消息内容（JSON）
	Status     int8      `gorm:"default:0;index" json:"status"`                // 0=待发送 1=已发送 2=发送失败
	RetryCount int       `gorm:"default:0" json:"retry_count"`                 // 已重试次数
	MaxRetry   int       `gorm:"default:5" json:"max_retry"`                   // 最大重试次数
	NextRetry  time.Time `gorm:"index" json:"next_retry"`                      // 下次重试时间
	ErrorMsg   string    `gorm:"type:varchar(255)" json:"error_msg"`           // 最后一次错误信息
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (LocalMessage) TableName() string {
	return "local_message"
}
