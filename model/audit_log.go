package model

import "time"

const (
	OperateTypeCreate = 1
	OperateTypeUpdate = 2
	OperateTypeDelete = 3

	ModuleStudent = "student"
	ModuleClass   = "class"
)

type AuditLog struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	TraceID     string    `gorm:"type:varchar(64);uniqueIndex" json:"trace_id"`
	OperateType int8      `gorm:"not null" json:"operate_type"`
	Module      string    `gorm:"type:varchar(32);not null" json:"module"`
	DataID      uint64    `gorm:"not null;index" json:"data_id"`
	Operator    string    `gorm:"type:varchar(32)" json:"operator"`
	BeforeData  string    `gorm:"type:json" json:"before_data"`
	AfterData   string    `gorm:"type:json" json:"after_data"`
	IP          string    `gorm:"type:varchar(32)" json:"ip"`
	CreatedAt   time.Time `json:"created_at"`
}

func (AuditLog) TableName() string {
	return "audit_log"
}
