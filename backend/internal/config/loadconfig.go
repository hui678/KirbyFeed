package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server              ServerConfig        `yaml:"server"`
	Database            DatabaseConfig      `yaml:"database"`
	Redis               RedisConfig         `yaml:"redis"`
	RabbitMQ            RabbitMQConfig      `yaml:"rabbitmq"`
	ObservabilityConfig ObservabilityConfig `yaml:"observability"`
	ObjectStorage       ObjectStorageConfig `yaml:"object_storage"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
	// AdminToken 用于保护 /admin/outbox/* 运维接口；为空时不注册这些路由。
	AdminToken string `yaml:"admin_token"`
}

// 数据库连接池的默认值。
//
// MaxOpenConns 必须显式设置：GORM 默认 MaxOpenConns = 0（无上限），缓存雪崩
// 或数据库变慢时会一路新建连接，直到打满 MySQL 的 max_connections —— 那会让
// 所有查询一起失败，包括本来能成功的那些。设了上限后，超出部分在应用侧排队
// 或失败，MySQL 自身仍然可用，这是最后一道、也是谁都绕不过去的硬闸门。
//
// DefaultMaxConcurrentDBLoad 是 Feed 回源闸门的容量，取连接池上限的一半：
// 连接池还要留给写路径、事务、迁移以及 social / video / like 等其他包的查询，
// 回源闸门只能占用其中一部分，不能吃满。
const (
	DefaultMaxOpenConns = 64
	DefaultMaxIdleConns = 16

	DefaultMaxConcurrentDBLoad = DefaultMaxOpenConns / 2

	// Redis 连接池默认值。默认的 10*GOMAXPROCS 在小机器上够用、在大机器上
	// 会开出远超 Redis 需要的连接；这里显式给出，并按池大小反推排队超时，
	// 避免缓存抖动时请求在池上无限等待（那时缓存已经从"加速"变成"拖慢"）。
	DefaultRedisPoolSize     = 64
	DefaultRedisMinIdleConns = 8
)

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	// MaxOpenConns / MaxIdleConns 为 0 时用上面的默认值。见 DefaultMaxOpenConns 的说明。
	MaxOpenConns int `yaml:"max_open_conns"`
	MaxIdleConns int `yaml:"max_idle_conns"`
}

// Normalize 把未配置的字段填成默认值，并修正 idle > open 这种非法组合。
func (c *DatabaseConfig) Normalize() {
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = DefaultMaxOpenConns
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = DefaultMaxIdleConns
	}
	if c.MaxIdleConns > c.MaxOpenConns {
		c.MaxIdleConns = c.MaxOpenConns
	}
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	// PoolSize / MinIdleConns 为 0 时用默认值，见 DefaultRedisPoolSize。
	PoolSize     int `yaml:"pool_size"`
	MinIdleConns int `yaml:"min_idle_conns"`
}

func (c *RedisConfig) Normalize() {
	if c.PoolSize <= 0 {
		c.PoolSize = DefaultRedisPoolSize
	}
	if c.MinIdleConns < 0 {
		c.MinIdleConns = 0
	}
	if c.MinIdleConns > c.PoolSize {
		c.MinIdleConns = c.PoolSize
	}
}

type RabbitMQConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type ObservabilityConfig struct {
	Pprof PprofConfig `yaml:"pprof"`
}
type PprofConfig struct {
	Enabled    bool   `yaml:"enabled"`
	ApiAddr    string `yaml:"api_addr"`
	WorkerAddr string `yaml:"worker_addr"`
}

// ObjectStorageConfig 对象存储配置。
//
// enabled=false（默认）时回落到本地磁盘，行为与改造前一致。
// provider 目前仅支持 "oss"（阿里云 OSS）。读取采用「私有桶 + 预签名 URL」，
// PresignTTL 控制 URL 有效期（秒，默认 3600）。
type ObjectStorageConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Provider    string `yaml:"provider"`
	Endpoint    string `yaml:"endpoint"`
	Bucket      string `yaml:"bucket"`
	Region      string `yaml:"region"`
	AccessKey   string `yaml:"access_key"`
	SecretKey   string `yaml:"secret_key"`
	PresignTTL  int    `yaml:"presign_ttl_seconds"`
}

// Normalize 填默认值。
func (c *ObjectStorageConfig) Normalize() {
	if c.Provider == "" {
		c.Provider = "oss"
	}
	if c.PresignTTL <= 0 {
		c.PresignTTL = 3600
	}
}

func Load(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", filename, err)
	}

	ApplyEnvOverrides(&cfg)
	return cfg, nil
}

func ApplyEnvOverrides(cfg *Config) {
	if cfg == nil {
		return
	}
	if v := os.Getenv("SERVER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = port
		}
	}
	if v := os.Getenv("ADMIN_TOKEN"); v != "" {
		cfg.Server.AdminToken = v
	}
	if v := os.Getenv("MYSQL_HOST"); v != "" {
		cfg.Database.Host = v
	}
	if v := os.Getenv("MYSQL_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Database.Port = port
		}
	}
	if v := os.Getenv("MYSQL_USER"); v != "" {
		cfg.Database.User = v
	}
	if v := os.Getenv("MYSQL_ROOT_PASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv("MYSQL_PASSWORD"); v != "" {
		cfg.Database.Password = v
	}
	if v := os.Getenv("MYSQL_DATABASE"); v != "" {
		cfg.Database.DBName = v
	}
	if v := os.Getenv("MYSQL_MAX_OPEN_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Database.MaxOpenConns = n
		}
	}
	if v := os.Getenv("MYSQL_MAX_IDLE_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Database.MaxIdleConns = n
		}
	}
	if v := os.Getenv("REDIS_HOST"); v != "" {
		cfg.Redis.Host = v
	}
	if v := os.Getenv("REDIS_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Redis.Port = port
		}
	}
	if v := os.Getenv("REDIS_PASSWORD"); v != "" {
		cfg.Redis.Password = v
	}
	if v := os.Getenv("REDIS_DB"); v != "" {
		if db, err := strconv.Atoi(v); err == nil {
			cfg.Redis.DB = db
		}
	}
	if v := os.Getenv("REDIS_POOL_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Redis.PoolSize = n
		}
	}
	if v := os.Getenv("REDIS_MIN_IDLE_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Redis.MinIdleConns = n
		}
	}
	if v := os.Getenv("RABBITMQ_HOST"); v != "" {
		cfg.RabbitMQ.Host = v
	}
	if v := os.Getenv("RABBITMQ_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.RabbitMQ.Port = port
		}
	}
	if v := os.Getenv("RABBITMQ_USER"); v != "" {
		cfg.RabbitMQ.Username = v
	}
	if v := os.Getenv("RABBITMQ_PASS"); v != "" {
		cfg.RabbitMQ.Password = v
	}

	if v := os.Getenv("OBJECT_STORAGE_ENABLED"); v != "" {
		cfg.ObjectStorage.Enabled = v == "1" || v == "true" || v == "yes"
	}
	if v := os.Getenv("OSS_PROVIDER"); v != "" {
		cfg.ObjectStorage.Provider = v
	}
	if v := os.Getenv("OSS_ENDPOINT"); v != "" {
		cfg.ObjectStorage.Endpoint = v
	}
	if v := os.Getenv("OSS_BUCKET"); v != "" {
		cfg.ObjectStorage.Bucket = v
	}
	if v := os.Getenv("OSS_REGION"); v != "" {
		cfg.ObjectStorage.Region = v
	}
	if v := os.Getenv("OSS_ACCESS_KEY"); v != "" {
		cfg.ObjectStorage.AccessKey = v
	}
	if v := os.Getenv("OSS_SECRET_KEY"); v != "" {
		cfg.ObjectStorage.SecretKey = v
	}
	if v := os.Getenv("OSS_PRESIGN_TTL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ObjectStorage.PresignTTL = n
		}
	}

	// 无论走 YAML 还是默认值，连接池参数都要保证有值（见 DefaultMaxOpenConns 的说明）
	cfg.Database.Normalize()
	cfg.Redis.Normalize()
	cfg.ObjectStorage.Normalize()
}

// bool用来表示是否使用了默认配置，true表示使用了默认配置
func LoadLocalDev(filename string) (Config, bool, error) {
	cfg, err := Load(filename)
	if err == nil {
		return cfg, false, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return DefaultLocalConfig(), true, nil
	}
	return Config{}, false, err
}

func DefaultLocalConfig() Config {
	cfg := Config{
		Server: ServerConfig{
			Port: 8080,
		},
		Database: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "123456",
			DBName:   "feedsystem",
		},
		Redis: RedisConfig{
			Host:     "localhost",
			Port:     6379,
			Password: "123456",
			DB:       0,
		},
		RabbitMQ: RabbitMQConfig{
			Host:     "localhost",
			Port:     5672,
			Username: "admin",
			Password: "password123",
		},
		ObservabilityConfig: ObservabilityConfig{
			Pprof: PprofConfig{
				Enabled:    true,
				ApiAddr:    "localhost:6060",
				WorkerAddr: "localhost:6061",
			},
		},
	}
	ApplyEnvOverrides(&cfg)
	return cfg
}
