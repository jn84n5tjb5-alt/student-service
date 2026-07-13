package utils

import (
	"math/rand"
	"time"

	"github.com/gin-gonic/gin"
)

func Success(c *gin.Context, data interface{}) {
	c.JSON(200, gin.H{
		"code":    200,
		"message": "操作成功",
		"data":    data,
	})
}

func Fail(c *gin.Context, code int, message string) {
	c.JSON(code, gin.H{
		"code":    code,
		"message": message,
		"data":    nil,
	})
}

// func QueryStudentList(tx *gorm.DB, query model.StudentListQuery) ([]model.Student, int64, error) {
// 	var students []model.Student
// 	var total int64

// 	if query.Name != "" {
// 		tx = tx.Where("name LIKE?", "%"+query.Name+"%")
// 	}
// 	if query.MinScore > 0 {
// 		tx = tx.Where("score>=?", query.MinScore)
// 	}
// 	if query.MaxScore > 0 {
// 		tx = tx.Where("score<=?", query.MaxScore)
// 	}
// 	switch query.Sort {
// 	case "score_asc":
// 		tx = tx.Order("score ASC")
// 	case "score_desc":
// 		tx = tx.Order("score DESC")
// 	case "id_asc":
// 		tx = tx.Order("id ASC")
// 	case "id_desc":
// 		tx = tx.Order("id DESC")
// 	default:
// 		tx = tx.Order("score DESC,id ASC")
// 	}
// 	if err := tx.Count(&total).Error; err != nil {
// 		return nil, 0, err
// 	}
// 	offset := (query.Page - 1) * query.PageSize
// 	err := tx.Limit(query.PageSize).Offset(offset).Find(&students).Error
// 	return students, total, err
// }

func GetRandomExpire(base time.Duration) time.Duration {
	offset := rand.Intn(300)
	return base + time.Duration(offset)*time.Second
}
