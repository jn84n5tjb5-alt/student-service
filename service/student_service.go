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

func GetStudentByName(name string) ([]model.Student, error) {
	if name == "" {
		return nil, errors.New("搜索关键词不能为空")
	}
	return dao.GetStudentByName(name)
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
	go func() {
		err := kafka.SendMessage(
			config.GlobalConfig.Kafka.TopicStudentEvent,
			fmt.Sprintf("%d", studendID),
			string(eventJSON),
		)
		if err != nil {
			logger.Errorf("发送学生变更事件失败：%v", err)
		}
	}()
}
func GetStudentByID(id int) (model.Student, error) {
	c := context.Background()
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
		return dao.GetStudentByID(id)
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
	student, err := dao.GetStudentByID(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return student, errors.New("学生不存在")
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
	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}
	if pageSize > 100 {
		pageSize = 100 // 防止恶意拉取大量数据
	}

	// 多查1条，用来判断是否还有下一页
	list, err := dao.GetDeletedStudentList(query.LastID, pageSize+1)
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

// func GetDeletStudentList(query model.StudentListQuery) ([]model.Student, int64, error) {
// 	if query.Page <= 0 {
// 		query.Page = 1
// 	}
// 	if query.PageSize <= 0 {
// 		query.PageSize = 10
// 	}
// 	students, total, err := dao.GetDeletedStudentList(query)
// 	if err != nil {
// 		return nil, 0, errors.New("查询失败")
// 	}

// 	return students, total, nil

// }

func GetStudentList(query model.StudentListQuery) ([]model.Student, int, bool, error) {
	// 业务层参数兜底
	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 10
	}
	if pageSize > 100 {
		pageSize = 100 // 防止恶意拉取大量数据
	}

	// 多查1条，用来判断是否还有下一页
	list, err := dao.GetStudentList(query.ClassID, query.LastID, pageSize+1)
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
	return dao.DB.Transaction(func(tx *gorm.DB) error {
		return dao.BatchCreateStudent(tx, students)
	})
}

func AddStudent(student *model.Student) error {
	_, err := dao.GetStudentByID(student.ID)
	if err == nil {
		return errors.New("学生已存在")
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.New("查询失败")
	}
	if err := dao.CreateStudent(student); err != nil {
		return err // 插入失败，直接返回，不发送事件
	}
	// 4. 发送审计消息（异步）
	go func() {
		auditMsg := &model.AuditMessage{
			TraceID:     uuid.New().String(),
			OperateType: model.OperateTypeCreate,
			Module:      model.ModuleStudent,
			DataID:      uint64(student.ID),
			Operator:    "admin",
			BeforeData:  "",
			AfterData:   student,
			IP:          "",
		}
		SendAuditMessage(auditMsg)
	}()
	pubilshStudentEvent("create", student.ID, student)
	return nil
}

func DeleteStudent(id int) (model.Student, error) {
	student, err := dao.GetStudentByID(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return student, errors.New("学生不存在")
		}
		return student, errors.New("查询失败")
	}
	err = dao.DeleteStudent(id)
	if err != nil {
		return student, errors.New("删除失败")
	}
	go func() {
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
		SendAuditMessage(auditMsg)
	}()
	c := context.Background()
	cacheKey := fmt.Sprintf("%s%d", studentCachePrefix, id)
	redis.Client.Del(c, cacheKey)
	pubilshStudentEvent("delete", student.ID, student)
	return student, nil
}

func UpdateStudent(id int, name string, score float64, classID int) (model.Student, error) {
	oldstudent, err := dao.GetStudentByID(id)
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
	err = dao.UpdateStudent(&student)
	if err != nil {
		return student, errors.New("更新失败")
	}
	go func() {
		auditMsg := &model.AuditMessage{
			TraceID:     uuid.New().String(),
			OperateType: model.OperateTypeUpdate,
			Module:      model.ModuleStudent,
			DataID:      uint64(id),
			Operator:    "admin",
			BeforeData:  oldstudent,
			AfterData:   student,
			IP:          "",
		}
		SendAuditMessage(auditMsg)
	}()
	c := context.Background()
	cacheKey := fmt.Sprintf("%s%d", studentCachePrefix, id)
	redis.Client.Del(c, cacheKey)
	go func() {
		time.Sleep(1 * time.Second)
		redis.Client.Del(c, cacheKey)
	}()
	pubilshStudentEvent("update", student.ID, student)
	return student, nil
}
func AddStudentScoreWithPessimisticLock(id int, addScore float64) error {
	tx := dao.DB.Begin()
	if tx.Error != nil {
		return errors.New("开启事务失败")
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	student, err := dao.GetStudentByIDForUpdate(tx, id)
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
	maxRetry := 3
	for i := 0; i < maxRetry; i++ {
		student, err := dao.GetStudentByID(id)
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
		rows, err := dao.UpdateScoreByVersion(id, newScore, student.Version)
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
