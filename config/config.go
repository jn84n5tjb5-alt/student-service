package config

import (
	"fmt"

	"github.com/spf13/viper"
)

var GlobalConfig *Config

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
	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("读取文件失败：%w", err)
	}
	if err := v.Unmarshal(&GlobalConfig); err != nil {
		return fmt.Errorf("解析文件失败：%w", err)
	}
	return nil
}
func (m *MysqlConfig) GetDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local", m.Username, m.Password, m.Host, m.Port, m.DBName, m.Charset)
}
