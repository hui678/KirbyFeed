package db

import (
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/message"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDB(dbcfg config.DatabaseConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		dbcfg.User, dbcfg.Password, dbcfg.Host, dbcfg.Port, dbcfg.DBName)

	dbcfg.Normalize() // 防御：即使配置没走 Load/Normalize，连接池参数也要有值

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// 必须显式设上限。GORM 默认 MaxOpenConns = 0（无上限），一旦缓存雪崩或
	// 数据库变慢，连接会被无限创建直到打满 MySQL 的 max_connections —— 那会
	// 让所有查询一起失败，包括本来能成功的。设了上限后超出部分在应用侧排队
	// 或失败，MySQL 自身仍然可用。详见 config.DefaultMaxOpenConns 的说明。
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(dbcfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(dbcfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return db, nil
}

func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&account.Account{}, &video.Video{}, &video.Like{}, &video.Comment{},
		&social.Social{}, &video.OutboxMsg{}, &video.Tag{}, &video.VideoTag{},
		&message.Message{}, &worker.Notification{},
	)
}

func CloseDB(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
