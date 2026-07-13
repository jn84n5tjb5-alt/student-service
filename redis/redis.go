package redis

import (
	"context"
	"fmt"
	"project/config"

	"github.com/redis/go-redis/v9"
)

var Client *redis.Client

func InitRedis() error {
	fmt.Printf("Redis配置: host=%s, port=%d, password=%s, db=%d\n",
		config.GlobalConfig.Redis.Host,
		config.GlobalConfig.Redis.Port,
		config.GlobalConfig.Redis.Password,
		config.GlobalConfig.Redis.DB,
	)
	Client = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.GlobalConfig.Redis.Host, config.GlobalConfig.Redis.Port),
		Password: config.GlobalConfig.Redis.Password,
		DB:       config.GlobalConfig.Redis.DB,
		PoolSize: config.GlobalConfig.Redis.PoolSize,
	})
	c := context.Background()
	_, err := Client.Ping(c).Result()
	if err != nil {
		fmt.Println("【原生错误详情】", err)
		return fmt.Errorf("redis连接失败:%w", err)
	}
	return nil
}
