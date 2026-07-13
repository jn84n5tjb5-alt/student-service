package dao

import "project/model"

func GetClassByID(id int) (model.Class, error) {
	var class model.Class
	err := DB.First(&class, id).Error
	return class, err
}
