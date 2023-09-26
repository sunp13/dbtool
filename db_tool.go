package dbtool

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var (
	JSON = jsoniter.ConfigCompatibleWithStandardLibrary
)

// 关于释放连接到连接池
// db.Ping() 调用完毕后会马上把连接返回给连接池。
// db.Exec() 调用完毕后会马上把连接返回给连接池，但是它返回的Result对象还保留这连接的引用，当后面的代码需要处理结果集的时候连接将会被重用。
// db.Query() 调用完毕后会将连接传递给sql.Rows类型，当然后者迭代完毕或者显示的调用.Close()方法后，连接将会被释放回到连接池。
// db.QueryRow()调用完毕后会将连接传递给sql.Row类型，当.Scan()方法调用之后把连接释放回到连接池。
// db.Begin() 调用完毕后将连接传递给sql.Tx类型对象，当.Commit()或.Rollback()方法调用后释放连接。

type mydb struct {
	alias   string // 数据库别名
	driver  string // godror/mysql
	debug   bool   // true/false
	ds      *sql.DB
	timeout time.Duration // 默认超时时间
}

// SetTimeout 设置默认超时时间
func (d *mydb) SetTimeout(timeout time.Duration) {
	d.timeout = timeout
}

// QuerySQL 普通查询 (oracle mysql 通用)
func (d *mydb) QuerySQL(s string, params []interface{}, timeout ...time.Duration) (data []map[string]interface{}, err error) {
	defer func() {
		if d.debug {
			pByte, _ := JSON.Marshal(params)
			L.Err(err).Str("alias", d.alias).Str("SQL", s).Bytes("PARAMS", pByte).Send()
		}
	}()

	// 超时ctx
	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	var rs *sql.Rows
	rs, err = d.ds.QueryContext(ctx, s, params...)
	if err != nil {
		return
	}
	// 释放连接
	defer rs.Close()
	data, err = rowsToMap(rs)
	return
}

// QuerySQL 普通DBLink查询 (oracle mysql 通用)
// 由于Oracle 通过DBlink查询会产生事务, 并不会主动释放, 会造成数据库连接数占用过高问题, 这里手动释放一下
func (d *mydb) QuerySQLWithDBLink(s string, params []interface{}, timeout ...time.Duration) (data []map[string]interface{}, err error) {
	defer func() {
		if d.debug {
			pByte, _ := JSON.Marshal(params)
			L.Err(err).Str("alias", d.alias).Str("SQL", s).Bytes("PARAMS", pByte).Send()
		}
	}()

	// 超时ctx
	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	// 开启事务
	var tx *sql.Tx
	tx, err = d.ds.Begin()
	if err != nil {
		return
	}
	// 主动释放事务
	defer tx.Rollback()

	var rs *sql.Rows
	rs, err = tx.QueryContext(ctx, s, params...)
	if err != nil {
		return
	}
	// 释放连接
	defer rs.Close()
	data, err = rowsToMap(rs)
	return
}

// UpdateSQL 普通更新 返回影响行数(oracle mysql 通用)
func (d *mydb) UpdateSQL(s string, params []interface{}, timeout ...time.Duration) (count int64, err error) {
	defer func() {
		if d.debug {
			pByte, _ := JSON.Marshal(params)
			L.Err(err).Str("alias", d.alias).Str("SQL", s).Bytes("PARAMS", pByte).Send()
		}
	}()

	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	var res sql.Result
	res, err = d.ds.ExecContext(ctx, s, params...)
	if err != nil {
		return
	}

	count, err = res.RowsAffected()
	return
}

// UpdateSQLMulti 同sql 操作多次 (oracle mysql 通用)
// timeout 是 事务整个超时时间
func (d *mydb) UpdateSQLMulti(s string, params [][]interface{}, timeout ...time.Duration) (count int64, err error) {
	defer func() {
		if d.debug {
			pByte, _ := JSON.Marshal(params)
			L.Err(err).Str("alias", d.alias).Str("SQL", s).Bytes("PARAMS", pByte).Send()
		}
	}()

	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	// 开启事务
	var tx *sql.Tx
	tx, err = d.ds.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()

	// prepare
	stmt, err := tx.Prepare(s)
	if err != nil {
		return
	}
	defer stmt.Close()

	var succ int64
	for _, v := range params {
		_, err = stmt.Exec(v...)
		if err != nil {
			return
		}
		succ++
	}
	// 全部执行完成后
	stmt.Close() // 在tx释放前 主动释放stmt
	tx.Commit()  // 提交并释放tx
	return
}

// UpdateSQLMultiErrRollback
// 批量更新,异常回滚
func (d *mydb) UpdateSQLMultiErrRollback(s string, params [][]interface{}, timeout ...time.Duration) (count int64, err error) {
	defer func() {
		if d.debug {
			pByte, _ := JSON.Marshal(params)
			L.Err(err).Str("alias", d.alias).Str("SQL", s).Bytes("PARAMS", pByte).Send()
		}
	}()

	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	// 开启事务
	var tx *sql.Tx
	tx, err = d.ds.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()

	// prepare
	stmt, err := tx.Prepare(s)
	if err != nil {
		return
	}
	defer stmt.Close()

	for _, v := range params {
		_, err = stmt.Exec(v...)
		if err != nil {
			break
		}
		count++
	}

	// 异常回滚
	if err != nil {
		tx.Rollback()
		return
	}
	// 正常提交
	tx.Commit()
	return
}

// AddSQL 添加数据返回主键id  (oracle 不能用)
func (d *mydb) AddSQL(s string, params []interface{}, timeout ...time.Duration) (pk int64, err error) {
	defer func() {
		if d.debug {
			pByte, _ := JSON.Marshal(params)
			L.Err(err).Str("alias", d.alias).Str("SQL", s).Bytes("PARAMS", pByte).Send()
		}
	}()

	if d.driver != "mysql" {
		err = fmt.Errorf("dedicated mysql")
		return
	}

	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	var res sql.Result
	res, err = d.ds.ExecContext(ctx, s, params...)
	if err != nil {
		return
	}
	pk, err = res.LastInsertId()
	return
}

// AddSQLOra 添加数据返回主键ID (GORACLE 专用,需要填写主键字段名)
func (d *mydb) AddSQLOra(s string, params []interface{}, pkName string, timeout ...time.Duration) (pk int64, err error) {
	defer func() {
		if d.debug {
			pByte, _ := JSON.Marshal(params)
			L.Err(err).Str("alias", d.alias).Str("SQL", s).Bytes("PARAMS", pByte).Send()
		}
	}()

	if d.driver != "godror" {
		err = fmt.Errorf("godror dedicated")
		return
	}
	if pkName == "" {
		err = fmt.Errorf("pkName is none")
		return
	}

	ctx, cancel := d.getTimeoutContext(timeout...)
	defer cancel()

	osql := fmt.Sprintf("%s returning %s into :%d", s, pkName, len(params)+1)
	params = append(params, sql.Out{Dest: &pk})
	_, err = d.ds.ExecContext(ctx, osql, params...)
	return
}

// 获取timeout context
func (d *mydb) getTimeoutContext(timeout ...time.Duration) (context.Context, context.CancelFunc) {
	ti := d.timeout
	if len(timeout) > 0 {
		ti = timeout[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), ti)
	return ctx, cancel
}

// // CallProcVoid 调用oracle 存储过程 通过 goracle 驱动
// func (d *mydb) CallProcVoid(qry string, params []interface{}, timeout ...time.Duration) error {

// 	if d.driver != "goracle" {
// 		return fmt.Errorf("goracle dedicated! %s", "")
// 	}
// 	// 超时ctx
// 	ctx, cancel := d.getTimeoutContext(timeout...)
// 	defer cancel()

// 	now := time.Now()
// 	_, err := d.ds.ExecContext(ctx, qry, params...)
// 	if d.debug {
// 		DLog.queryLog(d.alias, "Exec", qry, now, err, ctx.Err(), params...)
// 	}
// 	if err != nil {
// 		if ctx.Err() != nil {
// 			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
// 		}
// 		return err
// 	}
// 	return nil
// }

// // CallProcRtnString 调用oracle 存储过程 返回字符串  通过 goracle 驱动
// func (d *mydb) CallProcRtnString(qry string, params []interface{}, timeout ...time.Duration) (string, error) {
// 	if d.driver != "goracle" {
// 		return "", fmt.Errorf("goracle dedicated! %s", "")
// 	}
// 	// 超时ctx
// 	ctx, cancel := d.getTimeoutContext(timeout...)
// 	defer cancel()

// 	var res string
// 	params = append(params, sql.Out{Dest: &res})

// 	now := time.Now()
// 	_, err := d.ds.ExecContext(ctx, qry, params...)
// 	if d.debug {
// 		DLog.queryLog(d.alias, "Exec", qry, now, err, ctx.Err(), params...)
// 	}
// 	if err != nil {
// 		if ctx.Err() != nil {
// 			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
// 		}
// 		return "", err
// 	}
// 	return res, nil
// }

// // CallProcRtnRows 调用oracle 存储过程 返回结果集  通过 goracle 驱动
// func (d *mydb) CallProcRtnRows(qry string, params []interface{}, timeout ...time.Duration) ([][]driver.Value, error) {
// 	if d.driver != "goracle" {
// 		return nil, fmt.Errorf("goracle dedicated! %s", "")
// 	}
// 	// 超时ctx
// 	ctx, cancel := d.getTimeoutContext(timeout...)
// 	defer cancel()

// 	var res driver.Rows
// 	params = append(params, sql.Out{Dest: &res})

// 	now := time.Now()
// 	_, err := d.ds.ExecContext(ctx, qry, params...)
// 	if d.debug {
// 		DLog.queryLog(d.alias, "Exec", qry, now, err, ctx.Err(), params...)
// 	}
// 	if err != nil {
// 		if ctx.Err() != nil {
// 			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
// 		}
// 		return nil, err
// 	}
// 	rparams := [][]driver.Value{}

// 	for {
// 		vals := make([]driver.Value, len(res.Columns()))
// 		if err := res.Next(vals); err != nil {
// 			if err == io.EOF { //read end
// 				break
// 			}
// 			if err != nil {
// 				break
// 			}
// 		}
// 		rparams = append(rparams, vals)
// 	}
// 	return rparams, nil
// }

// // CallProc 调用存储过程 返回对应的结果集，需要注意返回数据中游标的处理，处理完成后需要关闭
// func (d *mydb) CallProc(qry string, params []interface{}, timeout ...time.Duration) error {
// 	if d.driver != "goracle" {
// 		return fmt.Errorf("goracle dedicated! %s", "")
// 	}
// 	// 超时ctx
// 	ctx, cancel := d.getTimeoutContext(timeout...)
// 	defer cancel()

// 	now := time.Now()
// 	_, err := d.ds.ExecContext(ctx, qry, params...)
// 	if d.debug {
// 		DLog.queryLog(d.alias, "Exec", qry, now, err, ctx.Err(), params...)
// 	}
// 	if err != nil {
// 		if ctx.Err() != nil {
// 			err = fmt.Errorf("%s ( %s )", err.Error(), ctx.Err().Error())
// 		}
// 		return err
// 	}
// 	return nil
// }
