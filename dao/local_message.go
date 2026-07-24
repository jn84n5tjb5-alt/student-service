package dao

import (
	"project/model"
	"time"

	"gorm.io/gorm"
)

func CreateLocalMessage(tx *gorm.DB, msg *model.LocalMessage) error {
	return tx.Create(msg).Error
}

func GetPendingMessages(limit int) ([]model.LocalMessage, error) {
	var messages []model.LocalMessage
	err := DB.Where("status=? AND next_retry<=?", 0, time.Now()).Order("id ASC").Limit(limit).Find(&messages).Error
	return messages, err
}

func MarkMessageSent(id uint64) error {
	return DB.Model(&model.LocalMessage{}).Where("id=?", id).Updates(map[string]interface{}{
		"status":     1,
		"updated_at": time.Now(),
	}).Error
}

func UpdateMessageStatus(id uint64, status int8, retryCount int, errMsg string) error {
	updates := map[string]interface{}{
		"status":      status,
		"retry_count": retryCount,
		"error_msg":   errMsg,
		"updated_at":  time.Now(),
	}
	if status == 0 && retryCount > 0 {
		backoff := time.Duration(5<<(retryCount-1)) * time.Second
		updates["next_retry"] = time.Now().Add(backoff)
	}
	return DB.Model(&model.LocalMessage{}).Where("id=?", id).Updates(updates).Error
}

func MarkMessageFailed(id uint64, errMsg string) error {
	return DB.Model(&model.LocalMessage{}).Where("id=?", id).Updates(map[string]interface{}{
		"status":     2,
		"error_msg":  errMsg,
		"updated_at": time.Now(),
	}).Error
}
