package service

import (
	"encoding/json"
	"io"
	"project/config"
	"project/kafka"
	"project/logger"
	"project/utils"
)

func ImportStudents(file io.Reader) (total, success int, err error) {
	students, err := utils.ParseStudentExcel(file)
	if err != nil {
		return 0, 0, err
	}
	total = len(students)
	success = 0

	for _, stu := range students {
		data, err := json.Marshal(stu)
		if err != nil {
			logger.Errorf("序列化学生数据失败：%v，数据*+v", err, stu)
			continue
		}
		err = kafka.SendMessage(
			config.GlobalConfig.Kafka.TopicImport,
			stu.Name,
			string(data),
		)
		if err != nil {
			logger.Errorf("发送导入消息失败：%v，学生：%s", err, stu.Name)
			continue
		}
		logger.Infof("导入消息发送成功: name=%s, topic=%s", stu.Name, config.GlobalConfig.Kafka.TopicImport)
		success++
	}
	return total, success, nil
}
