package middleware

import (
	"net/http"
	"project/utils"
	"sync"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

type IprateLimiter struct {
	limiterMap map[string]*rate.Limiter
	mu         *sync.RWMutex
	rate       rate.Limit
	burst      int
}

func NewIPRateLimiter(r rate.Limit, b int) *IprateLimiter {
	return &IprateLimiter{
		limiterMap: make(map[string]*rate.Limiter),
		mu:         &sync.RWMutex{},
		rate:       r,
		burst:      b,
	}
}

func (i *IprateLimiter) getLimiter(ip string) *rate.Limiter {
	i.mu.Lock()
	defer i.mu.Unlock()

	limiter, exists := i.limiterMap[ip]
	if !exists {
		limiter = rate.NewLimiter(i.rate, i.burst)
		i.limiterMap[ip] = limiter
	}
	return limiter
}

func RateLimitMiddleware(limiter *IprateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		l := limiter.getLimiter(ip)

		if !l.Allow() {
			utils.Fail(c, http.StatusTooManyRequests, "请求过多，稍候重试")
			c.Abort()
			return
		}
		c.Next()
	}
}
