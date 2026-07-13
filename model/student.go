package model

import "gorm.io/gorm"

type Student struct {
	ID        int            `json:"id" gorm:"primaryKey;autoIncrement"`
	Name      string         `json:"name" form:"name" gorm:"type:varchar(20);not null"`
	Score     float64        `json:"score" gorm:"type:decimal(5,2);not null"`
	ClassID   int            `json:"class_id"`
	Class     *Class         `json:"class,omitempty" gorm:"foreignKey:ClassID"`
	Version   int            `gorm:"column:version;default:0"`
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`
}

// type StudentListQuery struct {
// 	Name     string  `form:"name"`
// 	MinScore float64 `form:"min_score"`
// 	MaxScore float64 `form:"max_score"`
// 	Page     int     `form:"page"`
// 	PageSize int     `form:"page_size"`
// 	Sort     string  `form:"sort"`
// }

type StudentEvent struct {
	EventID   string      `json:"event_id"`
	EventType string      `json:"event_type"` // create/update/delete
	StudentID int         `json:"student_id"`
	Data      interface{} `json:"data,omitempty"`
	Time      int64       `json:"time"`
}
type StudentListQuery struct {
	ClassID  int `json:"class_id"`
	PageSize int `json:"page_size"`
	LastID   int `json:"last_id"`
}
type CursorPageResult struct {
	List     interface{} `json:"list"`
	PageSize int         `json:"page_size"`
	LastID   int         `json:"last_id"`  // 当前页最后一条ID，前端用来拉下一页
	HasMore  bool        `json:"has_more"` // 是否还有下一页
}
type ScoreAddRequest struct {
	AddScore float64 `json:"add_score" binding:"required"`
}

// AuditMessage 审计消息体
type AuditMessage struct {
	TraceID     string      `json:"trace_id"`
	OperateType int8        `json:"operate_type"` // 1新增 2修改 3删除
	Module      string      `json:"module"`
	DataID      uint64      `json:"data_id"`
	Operator    string      `json:"operator"`
	BeforeData  interface{} `json:"before_data"`
	AfterData   interface{} `json:"after_data"`
	IP          string      `json:"ip"`
}
