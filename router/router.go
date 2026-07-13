package router

import (
	"project/controller"
	"project/middleware"

	"github.com/gin-gonic/gin"
)

func InitRouter() *gin.Engine {
	r := gin.New()
	r.Use(middleware.RequestLogger())
	r.Use(gin.Recovery())
	studentGroup := r.Group("/api/v1/student")
	{
		studentGroup.GET("", controller.GetAllStudents)
		studentGroup.GET("/get/:id", controller.Getstudent)
		studentGroup.GET("get/de", controller.GetDeleteStudents)
		studentGroup.GET("/search", controller.GetStudentByName)

		authGroup := studentGroup.Group("")
		authGroup.Use(middleware.AuthMiddleware())
		{
			authGroup.POST("", controller.AddStudent)
			authGroup.PUT("/:id", controller.UpdateStudent)
			authGroup.DELETE("/:id", controller.DeleteStudent)
			authGroup.POST("/:id/score/pessimistic", controller.AddStudentScorePessimistic)
			authGroup.POST("/:id/score/optimistic", controller.AddStudentScoreOptimistic)
		}

	}
	return r
}
