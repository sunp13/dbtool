package dbtool

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"time"
)

// 关于释放连接到连接池
// db.Ping() 调用完毕后会马上把连接返回给连接池。
// db.Exec() 调用完毕后会马上把连接返回给连接池，但是它返回的Result对象还保留这连接的引用，当后面的代码需要处理结果集的时候连接将会被重用。
// db.Query() 调用完毕后会将连接传递给sql.Rows类型，当然后者迭代完毕或者显示的调用.Close()方法后，连接将会被释放回到连接池。
// db.QueryRow()调用完毕后会将连接传递给sql.Row类型，当.Scan()方法调用之后把连接释放回到连接池。
// db.Begin() 调用完毕后将连接传递给sql.Tx类型对象，当.Commit()或.Rollback()方法调用后释放连接。

type dbtool struct {
	alias   string // 数据库别名
	driver  string // goracle/mysql
	debug   bool   // true/false
	ds      *sql.DB
	timeout time.Duration // 默认超时时间
}

// SetTimeout 设置默认超时时间
func (d *dbtool) SetTimeout(timeout time.Duration) {
	d.timeout = timeout
}

// QuerySQL 普通查询 (oracle mysql 通用)
func (d *dbtool) QuerySQL(sql string, params []interface{}, timeout ...time.Duration) ([]map[string]interface{}, error) {
	// 超时ctx
	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	now := time.Now()
	rs, err := d.ds.QueryContext(ctx, sql, params...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	data, err := rowsToMap(rs)
	if d.debug {
		DLog.queryLog(d.alias, "Query", sql, now, err, ctx.Err(), params...)
	}

	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
		}
		return nil, err
	}
	return data, nil
}

// UpdateSQL 普通更新 返回影响行数(oracle mysql 通用)
func (d *dbtool) UpdateSQL(sql string, params []interface{}, timeout ...time.Duration) (int64, error) {
	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	now := time.Now()
	res, err := d.ds.ExecContext(ctx, sql, params...)
	if d.debug {
		DLog.queryLog(d.alias, "Exec", sql, now, err, ctx.Err(), params...)
	}
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
		}
		return 0, err
	}
	affectedRow, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affectedRow, nil
}

// UpdateSQLMulti 同sql 操作多次 (oracle mysql 通用)
func (d *dbtool) UpdateSQLMulti(sql string, params [][]interface{}, timeout ...time.Duration) (int64, error) {
	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	tx, err := d.ds.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	now := time.Now()
	var succ int64
	for _, v := range params {
		_, err := stmt.Exec(v...)
		if d.debug {
			DLog.queryLog(d.alias, "Exec", sql, now, err, ctx.Err(), v...)
		}
		if err != nil {
			// 在tx释放连接前关闭statment 防止stmt泄露
			stmt.Close()
			if ctx.Err() != nil {
				err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
			}
			// 这里返回就会tx.Rollback()
			return 0, err
		}
		succ++
	}
	// 全部执行完成后
	stmt.Close() // 在tx释放前 主动释放stmt
	tx.Commit()  // 提交并释放tx
	return succ, nil
}

// AddSQL 添加数据返回主键id  (oracle 不能用)
func (d *dbtool) AddSQL(sql string, params []interface{}, timeout ...time.Duration) (int64, error) {
	if d.driver == "goracle" {
		return 0, fmt.Errorf("Not support %s", d.driver)
	}

	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	now := time.Now()
	res, err := d.ds.ExecContext(ctx, sql, params...)
	DLog.queryLog(d.alias, "Exec", sql, now, err, ctx.Err(), params...)
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
		}
		return 0, err
	}
	lastInsert, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return lastInsert, nil
}

// AddSQLOra 添加数据返回主键ID (GORACLE 专用,需要填写主键字段名)
func (d *dbtool) AddSQLOra(s string, params []interface{}, pkName string, timeout ...time.Duration) (int64, error) {
	if d.driver != "goracle" {
		return 0, fmt.Errorf("goracle dedicated! %s", "")
	}
	if "" == pkName {
		return 0, fmt.Errorf("need a primary key field name")
	}

	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	osql := fmt.Sprintf("%s returning %s into :%d", s, pkName, len(params)+1)
	var id int64
	params = append(params, sql.Out{Dest: &id})

	now := time.Now()
	_, err := d.ds.ExecContext(ctx, osql, params...)
	DLog.queryLog(d.alias, "Exec", osql, now, err, ctx.Err(), params...)
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
		}
		return 0, err
	}
	return id, nil
}

// 获取timeout context
func (d *dbtool) getTimeoutContext(timeout ...time.Duration) (context.Context, context.CancelFunc) {
	ti := d.timeout
	if len(timeout) > 0 {
		ti = timeout[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), ti)
	return ctx, cancel
}

// CallProcVoid 调用oracle 存储过程 通过 goracle 驱动
func (d *dbtool) CallProcVoid(qry string, params []interface{}, timeout ...time.Duration) error {

	if d.driver != "goracle" {
		return fmt.Errorf("goracle dedicated! %s", "")
	}
	// 超时ctx
	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	now := time.Now()
	_, err := d.ds.ExecContext(ctx, qry, params...)
	if d.debug {
		DLog.queryLog(d.alias, "Exec", qry, now, err, ctx.Err(), params...)
	}
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
		}
		return err
	}
	return nil
}

// CallProcRtnString 调用oracle 存储过程 返回字符串  通过 goracle 驱动
func (d *dbtool) CallProcRtnString(qry string, params []interface{}, timeout ...time.Duration) (string, error) {
	if d.driver != "goracle" {
		return "", fmt.Errorf("goracle dedicated! %s", "")
	}
	// 超时ctx
	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	var res string
	params = append(params, sql.Out{Dest: &res})

	now := time.Now()
	_, err := d.ds.ExecContext(ctx, qry, params...)
	if d.debug {
		DLog.queryLog(d.alias, "Exec", qry, now, err, ctx.Err(), params...)
	}
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
		}
		return "", err
	}
	return res, nil
}

// CallProcRtnRows 调用oracle 存储过程 返回结果集  通过 goracle 驱动
func (d *dbtool) CallProcRtnRows(qry string, params []interface{}, timeout ...time.Duration) ([][]driver.Value, error) {
	if d.driver != "goracle" {
		return nil, fmt.Errorf("goracle dedicated! %s", "")
	}
	// 超时ctx
	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	var res driver.Rows
	params = append(params, sql.Out{Dest: &res})

	now := time.Now()
	_, err := d.ds.ExecContext(ctx, qry, params...)
	if d.debug {
		DLog.queryLog(d.alias, "Exec", qry, now, err, ctx.Err(), params...)
	}
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
		}
		return nil, err
	}
	rparams := [][]driver.Value{}

	for {
		vals := make([]driver.Value, len(res.Columns()))
		if err := res.Next(vals); err != nil {
			if err == io.EOF { //read end
				err = res.Close()
				break
			}
		}
		rparams = append(rparams, vals)
	}
	return rparams, nil
}
