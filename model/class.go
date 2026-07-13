package model

import "gorm.io/gorm"

type Class struct {
	ID        int            `json:"id" gorm:"primaryKey;autoIncrement"`
	Name      string         `json:"name" gorm:"type:varchar(30);not null;unique"`
	Students  []Student      `json:"students,omitempty" gorm:"foreignKey:ClassID"`
	DeletedAt gorm.DeletedAt `json:"deleted_at" gorm:"index"`
}
