package dbtool

import (
	"database/sql"
	"fmt"
	"time"
)

// rowsToMap func
func rowsToMap(rs *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rs.Columns()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	// 最终结果数组
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for i := 0; i < count; i++ {
		valuePtrs[i] = &values[i]
	}
	for rs.Next() {
		err := rs.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}
		entry := make(map[string]interface{})
		for i, col := range columns {
			switch val := values[i].(type) {
			case time.Time:
				entry[col] = func() interface{} {
					if val.IsZero() {
						return nil
					}
					return val.Format("2006-01-02 15:04:05")
				}()
			case []uint8:
				entry[col] = string(val)
			case int64:
				entry[col] = fmt.Sprintf("%d", val)
			default:
				entry[col] = val
			}
		}
		tableData = append(tableData, entry)
	}
	return tableData, nil
}

func arrayContain(str string, arr []string) bool {
	for _, v := range arr {
		if str == v {
			return true
		}
	}
	return false
}
