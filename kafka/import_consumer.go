package kafka

import (
	"context"
	"encoding/json"
	"project/config"
	"project/dao"
	"project/logger"
	"project/model"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ImportHandler 导入消息处理器
// 负责消费 student_import 主题的消息，批量写入数据库
type ImportHandler struct {
	BatchSize int             // 每批处理的条数，默认100
	pending   []model.Student // 未刷新的消息缓冲
	mu        sync.Mutex      // 保护 pending 的并发安全
}

// Setup 消费者启动前的初始化回调
func (h *ImportHandler) Setup(sarama.ConsumerGroupSession) error {
	if h.BatchSize <= 0 {
		h.BatchSize = 100 // 默认每批100条
	}
	h.pending = make([]model.Student, 0, h.BatchSize)
	logger.Info("【导入消费者】初始化完成，批量大小: ", h.BatchSize)
	return nil
}

// Cleanup 消费者结束前的清理回调，用于刷新剩余批次
func (h *ImportHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.pending) > 0 {
		logger.Infof("【导入消费者】Cleanup 刷新剩余 %d 条数据", len(h.pending))
		if err := h.flushBatch(h.pending); err != nil {
			logger.Errorf("【导入消费者】Cleanup 刷新失败: %v", err)
		} else {
			logger.Infof("【导入消费者】Cleanup 刷新成功 %d 条", len(h.pending))
		}
		h.pending = h.pending[:0]
	}
	logger.Info("【导入消费者】Cleanup 完成")
	return nil
}

// ConsumeClaim 核心消费逻辑
// 每条消息到来时，Kafka会调用此方法
func (h *ImportHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logger.Info("【导入消费者】ConsumeClaim 开始运行")
	logger.Infof("【导入消费者】分区: %d, 初始 offset: %d", claim.Partition(), claim.InitialOffset())

	h.mu.Lock()
	h.pending = make([]model.Student, 0, h.BatchSize)
	h.mu.Unlock()

	// 设置超时通道（例如 10 秒无新消息则退出）
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				// 通道关闭，退出
				goto flushAndExit
			}
			// 重置超时定时器（有消息到来）
			timeout.Reset(10 * time.Second)

			var student model.Student
			if err := json.Unmarshal(msg.Value, &student); err != nil {
				logger.Errorf("解析失败: %v", err)
				sess.MarkMessage(msg, "")
				continue
			}

			h.mu.Lock()
			h.pending = append(h.pending, student)
			if len(h.pending) >= h.BatchSize {
				batch := h.pending
				h.pending = make([]model.Student, 0, h.BatchSize)
				h.mu.Unlock()
				if err := h.flushBatch(batch); err != nil {
					logger.Errorf("批量插入失败: %v", err)
				}
			} else {
				h.mu.Unlock()
			}
			sess.MarkMessage(msg, "")

		case <-timeout.C:
			// 无新消息超时，主动退出
			logger.Info("【导入消费者】等待新消息超时，主动退出")
			goto flushAndExit
		}
	}

flushAndExit:
	// 刷新剩余数据
	h.mu.Lock()
	if len(h.pending) > 0 {
		batch := h.pending
		h.pending = nil
		h.mu.Unlock()
		logger.Infof("【导入消费者】超时退出前刷新剩余 %d 条数据", len(batch))
		if err := h.flushBatch(batch); err != nil {
			logger.Errorf("超时退出前刷新失败: %v", err)
		}
	} else {
		h.mu.Unlock()
	}
	logger.Info("【导入消费者】ConsumeClaim 退出")
	return nil
}

// flushBatch 执行批量插入
// 使用GORM的批量创建，一次性插入多条记录
func (h *ImportHandler) flushBatch(students []model.Student) error {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if len(students) == 0 {
		return nil
	}

	start := time.Now()
	logger.Infof("【导入消费者】准备插入 %d 条数据", len(students))

	// 使用批量插入（每次最多500条，避免SQL语句过长）
	err := dao.BatchCreateStudent(c, dao.DB, students)
	if err != nil {
		logger.Errorf("【导入消费者】批量插入 %d 条失败: %v", len(students), err)
		return err
	}

	logger.Infof("【导入消费者】成功插入 %d 条学生数据，耗时: %v", len(students), time.Since(start))
	return nil
}

// StartImportConsumer 启动导入消费者
// 在 main.go 中调用
func StartImportConsumer(c context.Context) error {
	topics := []string{config.GlobalConfig.Kafka.TopicImport}
	// 使用全新的 GroupID，例如在原有基础上加 "-new"
	groupID := config.GlobalConfig.Kafka.GroupID + "-import-new"
	return StartConsumer(c, topics, &ImportHandler{BatchSize: 100}, groupID)
}
