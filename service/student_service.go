package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"project/config"
	"project/dao"
	"project/kafka"
	"project/logger"
	"project/model"
	"project/redis"
	"project/utils"
	"sync"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	studentCachePrefix    = "student:info:"
	studentCacheExpire    = 10 * time.Minute
	studentNilCacheExpire = 1 * time.Minute
	studentLockPrefix     = "student:lock:"
	lockExpire            = 5 * time.Second
)

var eventPool *utils.Pool
var auditMsgPool = sync.Pool{
	New: func() interface{} {
		return &model.AuditMessage{}
	},
}

func InitEventPool(workerNum int, queueSize int) {
	eventPool = utils.NewPool(workerNum, queueSize)
	logger.Infof("协程池初始化完成：worker=%d, queue=%d", workerNum, queueSize)
}

func ShutdownEventPool() {
	if eventPool != nil {
		eventPool.Shutdown()
		logger.Info("协程池已关闭")
	}
}
func buildAuditMessage(operateType int8, module string, dataID uint64, operator string, beforeData, afterData interface{}) *model.AuditMessage {
	msg := auditMsgPool.Get().(*model.AuditMessage)
	msg.TraceID = uuid.New().String()
	msg.OperateType = operateType
	msg.Module = module
	msg.DataID = dataID
	msg.Operator = operator
	msg.BeforeData = beforeData
	msg.AfterData = afterData
	msg.IP = ""
	fmt.Printf("从池子里拿了一个对象，地址: %p\n", msg)
	return msg
}

func GetStudentByName(name string) ([]model.Student, error) {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel() // 重要：用完取消，释放资源
	if name == "" {
		return nil, errors.New("搜索关键词不能为空")
	}
	return dao.GetStudentByName(c, name)
}
func pubilshStudentEvent(eventType string, studendID int, data interface{}) {
	event := model.StudentEvent{
		EventID:   uuid.New().String(),
		EventType: eventType,
		StudentID: studendID,
		Data:      data,
		Time:      time.Now().Unix(),
	}
	eventJSON, _ := json.Marshal(event)
	err := eventPool.Submit(func() {
		err := kafka.SendMessageWithTimeout(
			config.GlobalConfig.Kafka.TopicStudentEvent,
			fmt.Sprintf("%d", studendID),
			string(eventJSON),
			2*time.Second,
		)
		if err != nil {
			logger.Errorf("发送学生变更事件失败：%v", err)
		}
	})
	if err != nil {
		logger.Errorf("提交任务到协程池失败：%v", err)
	}
}
func GetStudentByID(id int) (model.Student, error) {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cacheKey := fmt.Sprintf("%s%d", studentCachePrefix, id)
	lockKey := fmt.Sprintf("%s%d", studentLockPrefix, id)
	cacheData, err := redis.Client.Get(c, cacheKey).Result()
	if err == nil {
		if cacheData == "nil" {
			var student model.Student
			return student, errors.New("redis缓存:学生不存在")
		}
		var student model.Student
		if json.Unmarshal([]byte(cacheData), &student) == nil {
			fmt.Println("redis发力")
			return student, nil
		}
	}
	ok, err := redis.Client.SetNX(c, lockKey, "1", lockExpire).Result()
	if err != nil {
		return dao.GetStudentByID(c, id)
	}
	if !ok {
		time.Sleep(100 * time.Millisecond)
		return GetStudentByID(id)
	}
	defer redis.Client.Del(c, lockKey)
	cacheData, err = redis.Client.Get(c, cacheKey).Result()
	if err == nil {
		var student model.Student
		if json.Unmarshal([]byte(cacheData), &student) == nil {
			return student, nil
		}
	}
	student, err := dao.GetStudentByID(c, id)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return student, errors.New("查询学生数据超时")
		}
		return student, errors.New("查询失败")
	}
	go func() {
		jsonData, _ := json.Marshal(student)
		redis.Client.Set(c, cacheKey, jsonData, utils.GetRandomExpire(studentCacheExpire))
	}()
	class, err := GetClassByID(student.ClassID)
	if err == nil {
		student.Class = &class
	}
	pubilshStudentEvent("get", student.ID, student)
	return student, nil
}

func GetDeletedStudentList(query model.StudentListQuery) ([]model.Student, int, bool, error) {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}
	if pageSize > 100 {
		pageSize = 100 // 防止恶意拉取大量数据
	}

	// 多查1条，用来判断是否还有下一页
	list, err := dao.GetDeletedStudentList(c, query.LastID, pageSize+1)
	if err != nil {
		return nil, 0, false, err
	}

	// 判断是否还有下一页
	hasMore := len(list) > pageSize
	if hasMore {
		list = list[:pageSize] // 截断多余的那条
	}

	// 计算当前页最后一条ID
	lastID := 0
	if len(list) > 0 {
		lastID = int(list[len(list)-1].ID)
	}

	// 可选：在这里组装班级缓存信息
	// for i := range list { ... }

	return list, lastID, hasMore, nil
}

func GetStudentList(query model.StudentListQuery) ([]model.Student, int, bool, error) {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// 业务层参数兜底
	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}
	if pageSize > 100 {
		pageSize = 100 // 防止恶意拉取大量数据
	}

	// 多查1条，用来判断是否还有下一页
	list, err := dao.GetStudentList(c, query.ClassID, query.LastID, pageSize+1)
	if err != nil {
		return nil, 0, false, err
	}

	// 判断是否还有下一页
	hasMore := len(list) > pageSize
	if hasMore {
		list = list[:pageSize] // 截断多余的那条
	}

	// 计算当前页最后一条ID
	lastID := 0
	if len(list) > 0 {
		lastID = int(list[len(list)-1].ID)
	}

	// 可选：在这里组装班级缓存信息
	// for i := range list { ... }

	return list, lastID, hasMore, nil
}

func BatchAddStudents(students []model.Student) error {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return dao.DB.Transaction(func(tx *gorm.DB) error {
		return dao.BatchCreateStudent(c, tx, students)
	})
}

func AddStudent(student *model.Student) error {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := dao.GetStudentByID(c, student.ID)
	if err == nil {
		return errors.New("学生已存在")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.New("查询失败")
	}

	// 4. 发送审计消息（异步）
	err = dao.DB.Transaction(func(tx *gorm.DB) error {
		if err := dao.CreateStudentWithTx(c, tx, student); err != nil {
			return err
		}
		auditMsg := buildAuditMessage(
			model.OperateTypeCreate,
			model.ModuleStudent,
			uint64(student.ID),
			"admin",
			nil,
			student,
		)
		return SaveAuditMessage(tx, auditMsg)
	})
	if err != nil {

		return err
	}
	pubilshStudentEvent("create", student.ID, student)
	return nil
}

func DeleteStudent(id int) (model.Student, error) {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	student, err := dao.GetStudentByID(c, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return student, errors.New("学生不存在")
		}
		return student, errors.New("查询失败")
	}
	err = dao.DB.Transaction(func(tx *gorm.DB) error {
		if err := dao.DeleteStudentWithTx(c, tx, id); err != nil {
			return err
		}
		auditMsg := &model.AuditMessage{
			TraceID:     uuid.New().String(),
			OperateType: model.OperateTypeDelete,
			Module:      model.ModuleStudent,
			DataID:      uint64(id),
			Operator:    "admin",
			BeforeData:  student,
			AfterData:   "",
			IP:          "",
		}
		return SaveAuditMessage(tx, auditMsg)
	})
	if err != nil {
		return student, errors.New("删除失败")
	}
	cacheKey := fmt.Sprintf("%s%d", studentCachePrefix, id)
	redis.Client.Del(c, cacheKey)
	pubilshStudentEvent("delete", student.ID, student)
	return student, nil
}

func UpdateStudent(id int, name string, score float64, classID int) (model.Student, error) {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	oldstudent, err := dao.GetStudentByID(c, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return oldstudent, errors.New("学生不存在")
		}
		return oldstudent, errors.New("查询失败")
	}
	student := oldstudent
	student.Name = name
	student.Score = score
	student.ClassID = classID
	err = dao.DB.Transaction(func(tx *gorm.DB) error {
		if err := dao.UpdateStudentWithTx(c, tx, &student); err != nil {
			return err
		}
		auditMsg := buildAuditMessage(
			model.OperateTypeUpdate,
			model.ModuleStudent,
			uint64(id),
			"admin",
			oldstudent,
			student,
		)
		return SaveAuditMessage(tx, auditMsg)
	})
	if err != nil {
		return student, errors.New("更新失败")
	}

	go func() {
		c := context.Background()
		cacheKey := fmt.Sprintf("%s%d", studentCachePrefix, id)
		redis.Client.Del(c, cacheKey)
		time.Sleep(1 * time.Second)
		redis.Client.Del(c, cacheKey)
	}()
	pubilshStudentEvent("update", student.ID, student)
	return student, nil
}
func AddStudentScoreWithPessimisticLock(id int, addScore float64) error {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tx := dao.DB.Begin()
	if tx.Error != nil {
		return errors.New("开启事务失败")
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	student, err := dao.GetStudentByIDForUpdate(c, tx, id)
	if err != nil {
		tx.Rollback()
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.New("学生不存在")
		}
		return errors.New("查询学生失败")
	}
	newScore := student.Score + addScore
	if newScore < 0 {
		tx.Rollback()
		return errors.New("分数不能为负")
	}
	student.Score = newScore
	// 临时调试
	err = tx.Debug().Save(&student).Error

	err = tx.Save(&student).Error
	if err != nil {
		tx.Rollback()
		return errors.New("更新分数失败")
	}
	if err := tx.Commit().Error; err != nil {
		return errors.New("提交事务失败")
	}
	return nil
}

func AddStudentScoreWithOptimisticLock(id int, addScore float64) error {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	maxRetry := 3
	for i := 0; i < maxRetry; i++ {
		student, err := dao.GetStudentByID(c, id)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("学生不存在")
			}
			return errors.New("查询学生失败")
		}
		newScore := student.Score + addScore
		if newScore < 0 {
			return errors.New("分数不能为负")
		}
		rows, err := dao.UpdateScoreByVersion(c, id, newScore, student.Version)
		if err != nil {
			return errors.New("更新失败")
		}
		if rows > 0 {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
	}
	return errors.New("并发冲突严重，重试失败，请稍后再试")
}
