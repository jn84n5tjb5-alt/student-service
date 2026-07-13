package middleware

import (
	"net/http"
	"project/config"
	"project/logger"
	"project/utils"

	"github.com/gin-gonic/gin"
)

func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader(config.GlobalConfig.Auth.TokenHeader)

		if token == "" {
			utils.Fail(c, http.StatusUnauthorized, "未携带登录凭证")
			c.Abort()
			return
		}
		if token != config.GlobalConfig.Auth.Token {
			utils.Fail(c, http.StatusUnauthorized, "登录凭证无效")
			c.Abort()
			return
		}
		c.Set("username", "admin")
		logger.Infof("用户 %s 访问接口 %s", "admin", c.Request.URL.Path)
		c.Next()
	}

}
