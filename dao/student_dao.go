package dao

import (
	"project/model"

	"gorm.io/gorm"
)

func GetStudentByID(id int) (model.Student, error) {
	var student model.Student
	err := DB.First(&student, id).Error
	return student, err
}
func GetStudentByName(name string) ([]model.Student, error) {
	var students []model.Student
	err := DB.Where("name LIKE ?", name+"%").Find(&students).Error
	return students, err
}

// func GetStudentList(query model.StudentListQuery) ([]model.Student, int64, error) {
// 	tx := DB.Model(&model.Student{}).Preload("Class")
// 	return utils.QueryStudentList(tx, query)

// }
func GetStudentList(classID int, lastID int, pageSize int) ([]model.Student, error) {
	var students []model.Student
	tx := DB.Model(&model.Student{})

	if classID > 0 {
		tx = tx.Where("class_id = ?", classID)
	}
	if lastID > 0 {
		tx = tx.Where("id > ?", lastID)
	}

	err := tx.Order("id ASC").Limit(pageSize).Find(&students).Error
	return students, err
}

func GetDeletedStudentList(lastID int, pageSize int) ([]model.Student, error) {
	var students []model.Student
	tx := DB.Model(&model.Student{}).Preload("Class").Unscoped().Where("deleted_at IS NOT NULL")
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

func CreateStudent(student *model.Student) error {
	return DB.Create(student).Error
}

// func UpdateStudent(student *model.Student) error {
// 	return DB.Save(student).Error
// }

func DeleteStudent(id int) error {
	return DB.Delete(&model.Student{}, id).Error
}

func BatchCreateStudent(tx *gorm.DB, students []model.Student) error {
	return tx.Create(&students).Error
}

//	func UpdateClass(class *model.Class) error {
//		return DB.Save(class).Error
//	}
//
// 修改后的 UpdateStudent（结构体方式，忽略零值）
func UpdateStudent(student *model.Student) error {
	// 注意：Updates 不会更新零值字段（如 0, ""），若需更新零值请用 map 或 Select
	return DB.Model(&model.Student{}).Where("id = ?", student.ID).Updates(student).Error
}

// 修改后的 UpdateClass
func UpdateClass(class *model.Class) error {
	return DB.Model(&model.Class{}).Where("id = ?", class.ID).Updates(class).Error
}

func GetStudentByIDForUpdate(tx *gorm.DB, id int) (model.Student, error) {
	var student model.Student
	err := tx.Set("gorm:query_option", "FOR UPDATE").First(&student, id).Error
	return student, err
}

func UpdateScoreByVersion(id int, newScore float64, oldVersion int) (int64, error) {
	res := DB.Model(&model.Student{}).Where("id=? AND version=?", id, oldVersion).Updates(map[string]interface{}{"score": newScore, "version": oldVersion + 1})
	return res.RowsAffected, res.Error
}
