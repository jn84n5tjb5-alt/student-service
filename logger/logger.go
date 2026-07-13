package logger

import (
	"os"
	"project/config"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var log *zap.SugaredLogger

func InitLogger() error {
	fileWriter := &lumberjack.Logger{
		Filename:   config.GlobalConfig.Logger.Filename,
		MaxSize:    config.GlobalConfig.Logger.MaxSize,
		MaxBackups: config.GlobalConfig.Logger.MaxBackups,
		MaxAge:     config.GlobalConfig.Logger.MaxAge,
		Compress:   config.GlobalConfig.Logger.Compress,
	}
	level := zapcore.InfoLevel
	switch config.GlobalConfig.Logger.Level {
	case "debug":
		level = zap.DebugLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	}
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     customTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	var cores []zapcore.Core
	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(fileWriter),
		level,
	)
	cores = append(cores, fileCore)
	if config.GlobalConfig.Logger.Console {
		consoleEncoderConfig := encoderConfig
		consoleEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		consoleCore := zapcore.NewCore(
			zapcore.NewConsoleEncoder(consoleEncoderConfig),
			zapcore.AddSync(os.Stdout),
			level,
		)
		cores = append(cores, consoleCore)
	}
	core := zapcore.NewTee(cores...)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	log = logger.Sugar()
	return nil
}

// customTimeEncoder 自定义时间格式，替换Zap默认的时间戳格式
func customTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05"))
}
func Sync() {
	_ = log.Sync()
}

func Info(args ...interface{}) {
	log.Info(args...)
}
func Infof(template string, args ...interface{}) {
	log.Infof(template, args...)
}
func Error(args ...interface{}) {
	log.Error(args...)
}
func Errorf(template string, args ...interface{}) {
	log.Errorf(template, args...)
}
func Debug(args ...interface{}) {
	log.Debug(args...)
}

func Debugf(template string, args ...interface{}) {
	log.Debugf(template, args...)
}
func Warn(args ...interface{}) {
	log.Warn(args...)
}
func Warnf(template string, args ...interface{}) {
	log.Warnf(template, args...)
}
