package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

var GlobalConfig *Config

func getEnvKey(field string) string {
	return strings.ToUpper(strings.ReplaceAll(field, ".", "_"))
}

type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Mysql  MysqlConfig  `mapstructure:"mysql"`
	Auth   AuthConfig   `mapstructure:"auth"`
	Logger LoggerConfig `mapstructure:"logger"`
	Redis  RedisConfig  `mapstructure:"redis"`
	Kafka  KafkaConfig  `mapstructure:"kafka"`
}
type KafkaConfig struct {
	Brokers           []string `mapstructure:"brokers"`
	GroupID           string   `mapstructure:"group_id"`
	TopicStudentEvent string   `mapstructure:"topic_student_event"`
	AckMode           string   `mapstructure:"ack_mode"`
	TopicAuditLog     string   `mapstructure:"topic_audit_log"`
	TopicImport       string   `mapstructure:"topic_import"`
}
type ServerConfig struct {
	Port string `mapstructure:"port"`
	Mode string `mapstructure:"mode"`
}

type MysqlConfig struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	DBName   string `mapstructure:"dbname"`
	Charset  string `mapstructure:"charset"`
}
type AuthConfig struct {
	Token       string `mapstructure:"token"`
	TokenHeader string `mapstructure:"token_header"`
}

type LoggerConfig struct {
	Filename   string `mapstructure:"filename"`
	Level      string `mapstructure:"level"`
	Console    bool   `mapstructure:"console"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`
}

type RedisConfig struct {
	Host     string
	Port     int
	Password string
	DB       int
	PoolSize int
}

func InitConfig(configPath string) error {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	// 1. 读取配置文件
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("读取文件失败：%w", err)
	}

	// 2. 解析到结构体
	if err := v.Unmarshal(&GlobalConfig); err != nil {
		return fmt.Errorf("解析文件失败：%w", err)
	}

	// 3. 手动从环境变量覆盖（优先级最高）
	// Kafka Brokers（数组，用逗号分隔）
	if brokers := os.Getenv("APP_KAFKA_BROKERS"); brokers != "" {
		parts := strings.Split(brokers, ",")
		// 去除可能的空格
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		GlobalConfig.Kafka.Brokers = parts
		fmt.Printf("【配置】Kafka Brokers 从环境变量覆盖: %v\n", parts)
	} else {
		fmt.Printf("【配置】Kafka Brokers 使用配置文件值: %v\n", GlobalConfig.Kafka.Brokers)
	}

	// MySQL Host
	if host := os.Getenv("APP_MYSQL_HOST"); host != "" {
		GlobalConfig.Mysql.Host = host
		fmt.Printf("【配置】MySQL Host 从环境变量覆盖: %s\n", host)
	}

	// Redis Host
	if host := os.Getenv("APP_REDIS_HOST"); host != "" {
		GlobalConfig.Redis.Host = host
		fmt.Printf("【配置】Redis Host 从环境变量覆盖: %s\n", host)
	}
	// 其他需要覆盖的配置项可以按需添加

	return nil
}
func (m *MysqlConfig) GetDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local", m.Username, m.Password, m.Host, m.Port, m.DBName, m.Charset)
}
