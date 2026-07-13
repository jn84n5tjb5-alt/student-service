package middleware

import (
	"project/logger"
	"time"

	"github.com/gin-gonic/gin"
)

func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method
		clientIP := c.ClientIP()
		c.Next()
		cost := time.Since(start)
		statusCode := c.Writer.Status()
		logger.Infof("| %3d | %13v | %15s | %s  %s",
			statusCode,
			cost,
			clientIP,
			method,
			path,
		)
	}
}
