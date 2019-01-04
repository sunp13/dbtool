package dbtool

import (
	"database/sql"
	"io"
	"io/ioutil"
	"os"
	"time"

	yaml "gopkg.in/yaml.v2"
)

var (
	// D tool default
	D *mydb
	// DS tool map
	DS map[string]*mydb
	// DLog logger
	DLog = NewLogger(os.Stdout)
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

func newDBTool(p *DsProperty) *mydb {
	ds, err := sql.Open(p.DriverName, p.URL)
	if err != nil {
		DLog.Println("open err", err.Error())
		return nil
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
	}
}

// SetLogger setNewLogger
func SetLogger(out io.Writer) {
	DLog = NewLogger(os.Stdout)
}

// Init 加载指定数据库配置文件
func Init(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	return resolveConf(data)
}

func resolveConf(data []byte) error {
	var ds []DsProperty
	err := yaml.Unmarshal(data, &ds)
	if err != nil {
		DLog.Println("conf parse error:", err.Error())
		return err
	}

	for _, v := range ds {
		u := newDBTool(&v)
		err := u.ds.Ping()
		if err != nil {
			DLog.Printf("%s Ping failed!\n", v.Alias)
			continue
		}
		DLog.Printf("%s Ping succ!", v.Alias)
		if v.Alias == "default" {
			D = u
			continue
		}
		DS[v.Alias] = u
	}
	return nil
}
