package router

import (
	"project/controller"
	"project/middleware"

	"net/http/pprof"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func InitRouter() *gin.Engine {
	r := gin.New()
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	pprofGroup := r.Group("/debug/pprof")
	{
		pprofGroup.GET("/", gin.WrapF(pprof.Index))
		pprofGroup.GET("/cmdline", gin.WrapF(pprof.Cmdline))
		pprofGroup.GET("/profile", gin.WrapF(pprof.Profile))
		pprofGroup.GET("/symbol", gin.WrapF(pprof.Symbol))
		pprofGroup.GET("/trace", gin.WrapF(pprof.Trace))
		pprofGroup.GET("/allocs", gin.WrapF(pprof.Handler("allocs").ServeHTTP))
		pprofGroup.GET("/block", gin.WrapF(pprof.Handler("block").ServeHTTP))
		pprofGroup.GET("/goroutine", gin.WrapF(pprof.Handler("goroutine").ServeHTTP))
		pprofGroup.GET("/heap", gin.WrapF(pprof.Handler("heap").ServeHTTP))
		pprofGroup.GET("/mutex", gin.WrapF(pprof.Handler("mutex").ServeHTTP))
		pprofGroup.GET("/threadcreate", gin.WrapF(pprof.Handler("threadcreate").ServeHTTP))
	}
	limiter := middleware.NewIPRateLimiter(10, 20)
	r.Use(middleware.RateLimitMiddleware(limiter))
	r.Use(middleware.PrometheusMiddleware())
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
			authGroup.POST("/import", controller.ImportStudents) // 新增：批量导入接口
		}

	}
	return r
}
