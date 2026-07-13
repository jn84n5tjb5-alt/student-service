package controller

import (
	"project/model"
	"project/service"
	"project/utils"
	"strconv"

	"github.com/gin-gonic/gin"
)

func GetStudentByName(c *gin.Context) {
	var query model.Student
	if err := c.ShouldBindQuery(&query); err != nil {
		utils.Fail(c, 400, "参数错误"+err.Error())
		return
	}
	students, err := service.GetStudentByName(query.Name)
	if err != nil {
		utils.Fail(c, 500, err.Error())
		return
	}
	utils.Success(c, students)
}
func GetAllStudents(c *gin.Context) {
	var query model.StudentListQuery
	if err := c.ShouldBindQuery(&query); err != nil {
		utils.Fail(c, 400, "参数错误"+err.Error())
		return
	}
	students, lastID, hasMore, err := service.GetStudentList(query)
	if err != nil {
		utils.Fail(c, 500, err.Error())
		return
	}
	utils.Success(c, model.CursorPageResult{
		List:     students,
		PageSize: query.PageSize,
		LastID:   lastID,
		HasMore:  hasMore,
	})
}

func Getstudent(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		utils.Fail(c, 400, "id格式错误")
		return
	}
	student, err := service.GetStudentByID(id)
	if err != nil {
		if err.Error() == "学生不存在" {
			utils.Fail(c, 404, err.Error())
			return
		}
		utils.Fail(c, 500, err.Error())
		return
	}
	utils.Success(c, student)
}
func GetDeleteStudents(c *gin.Context) {
	var query model.StudentListQuery
	if err := c.ShouldBindQuery(&query); err != nil {
		utils.Fail(c, 400, "参数错误"+err.Error())
		return
	}
	students, lastID, hasMore, err := service.GetDeletedStudentList(query)
	if err != nil {
		utils.Fail(c, 500, err.Error())
		return
	}
	utils.Success(c, model.CursorPageResult{
		List:     students,
		PageSize: query.PageSize,
		LastID:   lastID,
		HasMore:  hasMore,
	})
}
func AddStudent(c *gin.Context) {
	// username, exists := c.Get("username")
	// if exists {
	// 	logger.Info("当前操作者：", username)
	// }
	var student model.Student
	if err := c.ShouldBindJSON(&student); err != nil {
		utils.Fail(c, 400, "参数错误"+err.Error())
		return
	}
	err := service.AddStudent(&student)
	if err != nil {
		if err.Error() == "学生已存在" {
			utils.Fail(c, 409, err.Error())
			return
		}
		utils.Fail(c, 500, err.Error())

	}
	utils.Success(c, student)
}

func UpdateStudent(c *gin.Context) {
	// username, exists := c.Get("username")
	// if exists {
	// 	logger.Info("当前操作者：", username)
	// }
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		utils.Fail(c, 400, "id格式错误")
		return
	}
	var updateStudent struct {
		Name    string  `json:"name" binding:"required,min=1,max=20"`
		Score   float64 `json:"score" binding:"required,gte=0,lte=100"`
		ClassID int     `json:"class_id" binding:"required"`
	}
	if err := c.ShouldBindJSON(&updateStudent); err != nil {
		utils.Fail(c, 400, "参数错误"+err.Error())
		return
	}
	student, err := service.UpdateStudent(id, updateStudent.Name, updateStudent.Score, updateStudent.ClassID)
	if err != nil {
		if err.Error() == "学生不存在" {
			utils.Fail(c, 404, err.Error())
			return
		}
		utils.Fail(c, 500, err.Error())
		return
	}
	utils.Success(c, student)
}

func DeleteStudent(c *gin.Context) {
	// username, exists := c.Get("username")
	// if exists {
	// 	logger.Info("当前操作者：", username)
	// }
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		utils.Fail(c, 400, "id格式错误")
		return
	}
	student, err := service.DeleteStudent(id)
	if err != nil {
		if err.Error() == "学生不存在" {
			utils.Fail(c, 404, err.Error())
			return
		}
		utils.Fail(c, 500, err.Error())
		return
	}
	utils.Success(c, student)

}
func AddStudentScorePessimistic(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		utils.Fail(c, 400, "id格式错误")
		return
	}
	var req model.ScoreAddRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		utils.Fail(c, 400, "参数错误")
		return
	}
	err = service.AddStudentScoreWithPessimisticLock(id, req.AddScore)
	if err != nil {
		utils.Fail(c, 500, err.Error())
		return
	}
	utils.Success(c, nil)
}
func AddStudentScoreOptimistic(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		utils.Fail(c, 400, "id格式错误")
		return
	}
	var req model.ScoreAddRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		utils.Fail(c, 400, "参数错误")
		return
	}
	err = service.AddStudentScoreWithOptimisticLock(id, req.AddScore)
	if err != nil {
		utils.Fail(c, 500, err.Error())
		return
	}
	utils.Success(c, nil)
}
