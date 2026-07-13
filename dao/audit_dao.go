package dao

import "project/model"

func CreateAuditLog(audit *model.AuditLog) error {
	return DB.Create(audit).Error
}
