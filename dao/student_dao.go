package dao

import (
	"context"
	"project/model"

	"gorm.io/gorm"
)

func GetStudentByID(c context.Context, id int) (model.Student, error) {
	var student model.Student
	err := DB.WithContext(c).First(&student, id).Error
	return student, err
}
func GetStudentByName(c context.Context, name string) ([]model.Student, error) {
	var students []model.Student
	err := DB.WithContext(c).Where("name LIKE ?", name+"%").Find(&students).Error
	return students, err
}

// func GetStudentList(query model.StudentListQuery) ([]model.Student, int64, error) {
// 	tx := DB.Model(&model.Student{}).Preload("Class")
// 	return utils.QueryStudentList(tx, query)

// }
func GetStudentList(c context.Context, classID int, lastID int, pageSize int) ([]model.Student, error) {
	var students []model.Student
	tx := DB.WithContext(c).Model(&model.Student{})

	if classID > 0 {
		tx = tx.Where("class_id = ?", classID)
	}
	if lastID > 0 {
		tx = tx.Where("id > ?", lastID)
	}

	err := tx.Order("id ASC").Limit(pageSize).Find(&students).Error
	return students, err
}

func GetDeletedStudentList(c context.Context, lastID int, pageSize int) ([]model.Student, error) {
	var students []model.Student
	tx := DB.WithContext(c).Model(&model.Student{}).Preload("Class").Unscoped().Where("deleted_at IS NOT NULL")
	if lastID > 0 {
		tx = tx.Where("id>?", lastID)
	}
	err := tx.Order("id ASC").Limit(pageSize).Find(&students).Error
	return students, err
}

// func GetDeletedStudentList(query model.StudentListQuery) ([]model.Student, int64, error) {

// 	tx := DB.Model(&model.Student{}).Preload("Class").Unscoped().Where("deleted_at IS NOT NULL")
// 	return utils.QueryS`tudentList(tx, query)
// }

func CreateStudent(c context.Context, student *model.Student) error {
	return DB.WithContext(c).Create(student).Error
}

// func UpdateStudent(student *model.Student) error {
// 	return DB.Save(student).Error
// }

func DeleteStudent(c context.Context, id int) error {
	return DB.WithContext(c).Delete(&model.Student{}, id).Error
}

func BatchCreateStudent(c context.Context, tx *gorm.DB, students []model.Student) error {
	return tx.WithContext(c).Create(&students).Error
}

//	func UpdateClass(class *model.Class) error {
//		return DB.Save(class).Error
//	}
//
// 修改后的 UpdateStudent（结构体方式，忽略零值）
func UpdateStudent(c context.Context, student *model.Student) error {
	// 注意：Updates 不会更新零值字段（如 0, ""），若需更新零值请用 map 或 Select
	return DB.WithContext(c).Model(&model.Student{}).Where("id = ?", student.ID).Updates(student).Error
}

// 修改后的 UpdateClass
func UpdateClass(c context.Context, class *model.Class) error {
	return DB.WithContext(c).Model(&model.Class{}).Where("id = ?", class.ID).Updates(class).Error
}

func GetStudentByIDForUpdate(c context.Context, tx *gorm.DB, id int) (model.Student, error) {
	var student model.Student
	err := tx.WithContext(c).Set("gorm:query_option", "FOR UPDATE").First(&student, id).Error
	return student, err
}

func UpdateScoreByVersion(c context.Context, id int, newScore float64, oldVersion int) (int64, error) {
	res := DB.WithContext(c).Model(&model.Student{}).Where("id=? AND version=?", id, oldVersion).Updates(map[string]interface{}{"score": newScore, "version": oldVersion + 1})
	return res.RowsAffected, res.Error
}

func CreateStudentWithTx(c context.Context, tx *gorm.DB, student *model.Student) error {
	return tx.WithContext(c).Create(student).Error
}

func UpdateStudentWithTx(c context.Context, tx *gorm.DB, student *model.Student) error {
	return tx.WithContext(c).Model(&model.Student{}).Where("id = ?", student.ID).Updates(student).Error
}
func DeleteStudentWithTx(c context.Context, tx *gorm.DB, id int) error {
	return tx.WithContext(c).Delete(&model.Student{}, id).Error
}
