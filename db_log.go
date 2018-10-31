package dbtool

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"
)

// Log struct
type Log struct {
	*log.Logger
}

// NewLogger new Logger
func NewLogger(out io.Writer) *Log {
	l := new(Log)
	l.Logger = log.New(out, "[DB] ", log.LstdFlags)
	return l
}

func (l *Log) queryLog(aliasName string, operator string, queryString string, t time.Time, err error, cerr error, args ...interface{}) {
	sub := time.Now().Sub(t) / 1e5
	elsp := float64(int(sub)) / 10.0

	flag := " OK"
	if err != nil {
		flag = " FAIL"
	}
	con := fmt.Sprintf(" - [QUERY/%s] - [%s / %11s / %7.1fms] - [%s]", aliasName, flag, operator, elsp, queryString)
	cons := make([]string, 0, len(args))
	for _, arg := range args {
		cons = append(cons, fmt.Sprintf("%v", arg))
	}
	if len(cons) > 0 {
		con += fmt.Sprintf(" - `%s`", strings.Join(cons, "`, `"))
	}
	if err != nil {
		con += " - " + err.Error()
	}

	if cerr != nil {
		con += "(" + cerr.Error() + ")"
	}
	l.Println(con)
}
