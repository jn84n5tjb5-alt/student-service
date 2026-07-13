package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"project/dao"
	"project/model"
	"project/redis"
	"project/utils"
	"time"

	"gorm.io/gorm"
)

const (
	classCachePrefix    = "class:info:"
	classCacheExpire    = 30 * time.Minute
	classNilCacheExpire = 2 * time.Minute
)

// GetClassByID 带缓存的班级查询
func GetClassByID(classID int) (model.Class, error) {
	ctx := context.Background()
	cacheKey := fmt.Sprintf("%s%d", classCachePrefix, classID)

	// 查缓存
	cacheData, err := redis.Client.Get(ctx, cacheKey).Result()
	if err == nil {
		if cacheData == "nil" {
			return model.Class{}, errors.New("班级不存在")
		}
		var class model.Class
		if json.Unmarshal([]byte(cacheData), &class) == nil {
			return class, nil
		}
	}

	// 查数据库
	class, err := dao.GetClassByID(classID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			go func() {
				redis.Client.Set(ctx, cacheKey, "nil", classNilCacheExpire)
			}()
			return class, errors.New("班级不存在")
		}
		return class, errors.New("班级查询失败")
	}

	// 回写缓存
	go func() {
		jsonData, _ := json.Marshal(class)
		redis.Client.Set(ctx, cacheKey, jsonData, utils.GetRandomExpire(classCacheExpire))
	}()

	return class, nil
}
