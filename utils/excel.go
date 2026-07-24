package utils

import (
	"errors"
	"io"
	"project/logger"
	"project/model"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

// ParseStudentExcel 解析 Excel 文件，返回学生列表
// 期望列顺序：姓名、分数、班级ID
// 分数和班级ID为可选列，缺省或无效值时使用默认值（0）
func ParseStudentExcel(reader io.Reader) ([]model.Student, error) {
	f, err := excelize.OpenReader(reader)
	if err != nil {
		return nil, errors.New("Excel文件格式错误: " + err.Error())
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	if sheetName == "" {
		return nil, errors.New("Excel文件为空")
	}

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, errors.New("读取Excel内容失败: " + err.Error())
	}
	if len(rows) < 2 {
		return nil, errors.New("数据行不能为空，请确保第一行为表头")
	}

	students := make([]model.Student, 0, len(rows)-1)

	// 从第2行开始遍历（第1行是表头）
	for i := 1; i < len(rows); i++ {
		row := rows[i]

		// 检查是否整行为空
		allEmpty := true
		for _, cell := range row {
			if strings.TrimSpace(cell) != "" {
				allEmpty = false
				break
			}
		}
		if allEmpty {
			continue
		}

		// 1. 姓名（必填，第一列）
		var name string
		if len(row) > 0 {
			name = strings.TrimSpace(row[0])
		}
		if name == "" {
			logger.Warnf("Excel第%d行姓名为空，跳过该行", i+1)
			continue
		}

		// 2. 分数（第二列，可选，默认为0）
		score := 0.0
		if len(row) > 1 && strings.TrimSpace(row[1]) != "" {
			scoreStr := strings.TrimSpace(row[1])
			if s, err := strconv.ParseFloat(scoreStr, 64); err == nil {
				if s >= 0 && s <= 100 {
					score = s
				} else {
					logger.Warnf("Excel第%d行分数超出范围(0-100)：%s，使用默认值0", i+1, scoreStr)
				}
			} else {
				logger.Warnf("Excel第%d行分数格式错误：%s，使用默认值0", i+1, scoreStr)
			}
		}

		// 3. 班级ID（第三列，可选，默认为0）
		classID := 0
		if len(row) > 2 && strings.TrimSpace(row[2]) != "" {
			classStr := strings.TrimSpace(row[2])
			if c, err := strconv.Atoi(classStr); err == nil {
				if c > 0 {
					classID = c
				} else {
					logger.Warnf("Excel第%d行班级ID必须大于0：%s，使用默认值0", i+1, classStr)
				}
			} else {
				logger.Warnf("Excel第%d行班级ID格式错误：%s，使用默认值0", i+1, classStr)
			}
		}

		logger.Infof("Excel第%d行解析成功：姓名=%s，分数=%.2f，班级ID=%d", i+1, name, score, classID)

		students = append(students, model.Student{
			Name:    name,
			Score:   score,
			ClassID: classID,
		})
	}

	if len(students) == 0 {
		return nil, errors.New("未解析到任何有效学生数据")
	}

	logger.Infof("Excel解析完成，共解析到 %d 条有效学生记录", len(students))
	return students, nil
}
