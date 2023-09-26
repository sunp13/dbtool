package dbtool

import (
	"database/sql"
	"os"
	"time"

	yaml "gopkg.in/yaml.v2"
)

var (
	// D tool default
	D *mydb
	// DS tool map
	DS map[string]*mydb
)

// DsProperty conf 文件实体
type DsProperty struct {
	Alias      string `yaml:"alias"`
	DriverName string `yaml:"driverName"`
	URL        string `yaml:"url"`
	MaxIdle    int    `yaml:"maxIdle"`
	MaxConn    int    `yaml:"maxConn"`
	Debug      bool   `yaml:"debug"`
}

func init() {
	// 环境变量for oracle
	os.Setenv("NLS_LANG", "SIMPLIFIED CHINESE_CHINA.UTF8")
	DS = make(map[string]*mydb, 0)
}

func newDBTool(p *DsProperty) (*mydb, error) {
	ds, err := sql.Open(p.DriverName, p.URL)
	if err != nil {
		return nil, err
	}
	ds.SetMaxIdleConns(p.MaxIdle)
	ds.SetMaxOpenConns(p.MaxConn)
	ds.SetConnMaxLifetime(30 * 60 * time.Second) // 30分钟以后的链接不复用,直接关掉,拿新的
	return &mydb{
		alias:   p.Alias,
		driver:  p.DriverName,
		debug:   p.Debug,
		ds:      ds,
		timeout: 30 * time.Second, // 默认所有请求30秒超时
	}, nil
}

// Init 加载指定数据库配置文件
func Init(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	if err := resolveConf(data); err != nil {
		return err
	}
	// 初始化日志-默认使用os.Stdout
	SetLogger(nil)
	return nil
}

func resolveConf(data []byte) error {
	var ds []DsProperty
	err := yaml.Unmarshal(data, &ds)
	if err != nil {
		return err
	}
	for _, v := range ds {
		u, err := newDBTool(&v)
		if err != nil {
			return err
		}
		if err = u.ds.Ping(); err != nil {
			return err
		}
		if v.Alias == "default" {
			D = u
			continue
		}
		DS[v.Alias] = u
	}
	return nil
}
