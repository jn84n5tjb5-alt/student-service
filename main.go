package main

import (
	"context"
	"project/config"
	"project/dao"
	"project/kafka"
	"project/logger"
	"project/redis"
	"project/router"
	"project/service"
	_ "time/tzdata"

	"github.com/gin-gonic/gin"
)

func main() {
	err := config.InitConfig("./config.yaml")
	if err != nil {
		panic("配置初始化失败:" + err.Error())
	}
	if err := logger.InitLogger(); err != nil {
		panic("日志初始化失败：" + err.Error())
	}
	defer logger.Sync()
	logger.Info("配置初始化成功")

	gin.SetMode(config.GlobalConfig.Server.Mode)

	dsn := config.GlobalConfig.Mysql.GetDSN()
	if err := dao.InitDB(dsn); err != nil {
		logger.Error("数据库连接失败：", err)
		return
	}
	logger.Info("数据库连接成功")
	if err := redis.InitRedis(); err != nil {
		logger.Error("redis连接失败:", err)
		return
	}
	logger.Info("redis连接成功")
	if err := kafka.InitKafka(); err != nil {
		logger.Errorf("kafka初始化失败：%v", err)
		return
	}
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := kafka.StartStudentConsumer(c); err != nil {
		logger.Errorf("kafka消费者启动失败:%v", err)
		return
	}
	if err = kafka.StartAuditConsumer(c); err != nil {
		logger.Errorf("审计消费者启动失败:%v", err)
		return
	}
	if err = kafka.StartImportConsumer(c); err != nil {
		logger.Errorf("导入消费者启动失败:%v", err)
		return
	}
	service.InitEventPool(100, 1000)
	defer service.ShutdownEventPool()
	go service.StartLocalMessageSender(context.Background())
	r := router.InitRouter()
	logger.Infof("服务器启动在%s", config.GlobalConfig.Server.Port)
	err = r.Run(config.GlobalConfig.Server.Port)
	if err != nil {
		logger.Error("服务器启动失败", err)
	}
}
