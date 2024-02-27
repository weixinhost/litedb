// Mysql数据库客户端
// 底层使用 database/sql 以及 github.com/go-sql-driver/mysql 实现
// LiteDB 的设计目标为轻量的数据库操作封装.无意于任何的复杂关系映射.
// 仅仅以一种舒服的姿势进行数据库与程序对象的映射.
// 并且对基本的操作提供基本的语法糖,方便操作.

package litedb

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"time"

	"github.com/go-sql-driver/mysql"
)

var Debug bool = false
var connMaxLiftTime time.Duration = 3600 * 6 * time.Second //默认连接过期时间，6个小时

/*
*
设置每个连接的最大生存时间。如果小于等于0 则用不过期。
*
*/
func SetConnMaxLifeTime(max time.Duration) {
	connMaxLiftTime = max
}

// 开启Debug模式
func OpenDebug() {
	Debug = true
}

// 关闭Debug模式
func CloseDebug() {
	Debug = false
}

// Sql操作集
type Sql struct {
	Exec  func(sqlFmt string, sqlValue ...interface{}) *ClientExecResult
	Query func(sqlFmt string, sqlValue ...interface{}) *ClientQueryResult
}

// 客户端
type Client struct {
	Sql
	Host         string
	Port         uint32
	User         string
	Password     string
	Database     string
	Protocol     string
	SSL          bool
	RootCertData []byte
	Config       *ClientDNSConfigure
	db           *sql.DB

	maxIdleConn     int
	maxConn         int
	connMaxLifetime time.Duration
}

// 事务客户端
type Transaction struct {
	Sql
	tx *sql.Tx
	db *sql.DB
}

// 对Struct类型的支持,使用 db tag 进行数据库字段映射
// 对Map类型会将value转换为string.请确保map类型中只包含基本数据类型
func (this *Sql) Insert(table string, v interface{}) *ClientExecResult {

	smap, err := StructToMap(v)
	r := new(ClientExecResult)

	if err != nil {

		r.Err = err
		return r
	}

	if len(smap) < 1 {
		r.Err = &SQLError{s: "nothing insert"}
		return r
	}

	keys := bytes.NewBufferString("")
	vals := bytes.NewBufferString("")

	valList := make([]interface{}, 0)

	for k, v := range smap {
		keys.WriteString(fmt.Sprintf("`%s`,", k))
		vals.WriteString("?,")
		valList = append(valList, v)
	}

	keysSplit := string(keys.Bytes()[0 : keys.Len()-1])
	valsSplit := string(vals.Bytes()[0 : vals.Len()-1])
	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s);", table, keysSplit, valsSplit)
	return this.Exec(sql, valList...)
}

// 对Struct类型的支持,使用 db tag 进行数据库字段映射
// 对Map类型会将value转换为string.请确保map类型中只包含基本数据类型
// where 条件写法 id = ?
func (this *Sql) Update(table string, v interface{}, whereFmt string, whereValue ...interface{}) *ClientExecResult {

	smap, err := StructToMap(v)
	r := new(ClientExecResult)

	if err != nil {
		r.Err = err
		return r
	}

	if len(smap) < 1 {
		r.Err = &SQLError{s: "nothing update"}
		return r
	}

	set := bytes.NewBufferString("")
	valList := make([]interface{}, 0)

	for k, v := range smap {
		set.WriteString(fmt.Sprintf("`%s`=?,", k))
		valList = append(valList, v)
	}
	setSplit := string(set.Bytes()[0 : set.Len()-1])
	sql := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", table, setSplit, whereFmt)
	valList = append(valList, whereValue...)
	return this.Exec(sql, valList...)

}

// map类型无必要使用该方法
// 部分字段更新
// 该接口的意义是struct类型为完整的数据库字段映射.但某些时候我们仅仅需要更新部分字段.此时,如果使用完整映射的进行更新操作
// 则更容易误覆盖.
// 因此提供了这个接口进行部分字段更新.
// fields 就是需要更新的数据库字段名称
// v,whereFmt,WhereValue 等值意义不变
func (this *Sql) UpdateFields(table string, v interface{}, fields []string, whereFmt string, whereValue ...interface{}) *ClientExecResult {

	smap, err := StructToMap(v)
	r := new(ClientExecResult)

	if err != nil {
		r.Err = err
		return r
	}

	if len(smap) < 1 {
		r.Err = &SQLError{s: "nothing update"}
		return r
	}

	vmap := make(map[string]string, 0)

	for _, f := range fields {
		v, ok := smap[f]
		if ok {
			vmap[f] = v
		}
	}

	smap = vmap

	set := bytes.NewBufferString("")
	valList := make([]interface{}, 0)

	for k, v := range smap {
		set.WriteString(fmt.Sprintf("`%s`=?,", k))
		valList = append(valList, v)
	}

	setSplit := string(set.Bytes()[0 : set.Len()-1])

	sql := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", table, setSplit, whereFmt)
	return this.Exec(sql, append(valList, whereValue...)...)

}

// 根据Where条件删除数据
func (this *Sql) Delete(table string, whereFmt string, whereValue ...interface{}) *ClientExecResult {
	sql := fmt.Sprintf("DELETE FROM `%s` WHERE %s", table, whereFmt)
	return this.Exec(sql, whereValue...)
}

// 插入或更新行(当主键已存在的时候)
// SQL语句为: INSERT INTO .... ON DUPLICATE KEY UPDATE ....
// 全部字段更新
func (this *Sql) InsertOrUpdate(table string, v interface{}) *ClientExecResult {

	smap, err := StructToMap(v)
	r := new(ClientExecResult)

	if err != nil {
		r.Err = err
		return r
	}

	if len(smap) < 1 {
		r.Err = &SQLError{s: "nothing insert"}
		return r

	}

	insertKeys := bytes.NewBufferString("")
	insertVals := bytes.NewBufferString("")

	set := bytes.NewBufferString("")

	insertValList := make([]interface{}, 0)
	updateValList := make([]interface{}, 0)

	for k, v := range smap {
		insertKeys.WriteString(fmt.Sprintf("`%s`,", k))
		insertVals.WriteString("?,")

		set.WriteString(fmt.Sprintf("`%s`=?,", k))
		insertValList = append(insertValList, v)
		updateValList = append(updateValList, v)
	}

	keysSplit := string(insertKeys.Bytes()[0 : insertKeys.Len()-1])
	valsSplit := string(insertVals.Bytes()[0 : insertVals.Len()-1])
	setSplit := string(set.Bytes()[0 : set.Len()-1])

	insertValList = append(insertValList, updateValList...)

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE  %s", table, keysSplit, valsSplit, setSplit)

	return this.Exec(sql, insertValList...)

}

// map类型无必要使用该方法
// 插入或更新行(当主键已存在的时候)
// SQL语句为: INSERT INTO .... ON DUPLICATE KEY UPDATE ....
// 可以指定更新字段
func (this *Sql) InsertOrUpdateFields(table string, v interface{}, updateFields ...string) *ClientExecResult {

	smap, err := StructToMap(v)
	r := new(ClientExecResult)

	if err != nil {
		r.Err = err
		return r
	}

	if len(smap) < 1 {
		r.Err = &SQLError{s: "nothing insert"}
		return r
	}

	if len(updateFields) < 1 {
		r.Err = &SQLError{s: "nothing update"}
		return r
	}

	updateMap := make(map[string]string, 0)
	for _, f := range updateFields {
		if v, ok := smap[f]; ok {
			updateMap[f] = v
		}
	}

	if len(updateMap) < 1 {
		r.Err = &SQLError{s: "nothing update"}
		return r
	}

	insertKeys := bytes.NewBufferString("")
	insertVals := bytes.NewBufferString("")

	set := bytes.NewBufferString("")

	insertValList := make([]interface{}, 0)
	updateValList := make([]interface{}, 0)

	for k, v := range smap {
		insertKeys.WriteString(fmt.Sprintf("`%s`,", k))
		insertVals.WriteString("?,")
		insertValList = append(insertValList, v)

	}

	for k, v := range updateMap {
		set.WriteString(fmt.Sprintf("`%s`=?,", k))
		updateValList = append(updateValList, v)
	}

	keysSplit := string(insertKeys.Bytes()[0 : insertKeys.Len()-1])
	valsSplit := string(insertVals.Bytes()[0 : insertVals.Len()-1])
	setSplit := string(set.Bytes()[0 : set.Len()-1])

	insertValList = append(insertValList, updateValList...)

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE  %s", table, keysSplit, valsSplit, setSplit)

	return this.Exec(sql, insertValList...)

}

// 批量插入
// SQL语句为: INSERT INTO `%s` (field,field) VALUES (?,?),(?,?)
func (this *Sql) BatchInsert(table string, vs interface{}) *ClientExecResult {

	r := new(ClientExecResult)

	list, err := ListStructToMap(vs)

	if err != nil {
		r.Err = err
		return r
	}
	valList := make([]interface{}, 0)
	sql := fmt.Sprintf("INSERT INTO `%s` ", table)

	smap := list[0]
	keys := bytes.NewBufferString("")

	keysIndex := []string{}

	for k, _ := range smap {
		keysIndex = append(keysIndex, k)
		keys.WriteString(fmt.Sprintf("`%s`,", k))
	}
	keysSplit := string(keys.Bytes()[0 : keys.Len()-1])

	sql += fmt.Sprintf("(%s) VALUES ", keysSplit)

	for _, smap := range list {
		vals := bytes.NewBufferString("")

		for i := 0; i < len(keysIndex); i++ {
			k := keysIndex[i]
			v := smap[k]
			vals.WriteString("?,")
			valList = append(valList, v)
		}

		valsSplit := string(vals.Bytes()[0 : vals.Len()-1])
		sql += fmt.Sprintf("(%s),", valsSplit)
	}

	sql = string([]byte(sql)[0 : len(sql)-1])

	return this.Exec(sql, valList...)

}

// 批量重置
// SQL语句为: REPLACE INTO `%s` (field,field) VALUES (?,?),(?,?)
func (this *Sql) BatchReplace(table string, vs interface{}) *ClientExecResult {

	r := new(ClientExecResult)

	list, err := ListStructToMap(vs)

	if err != nil {
		r.Err = err
		return r
	}
	valList := make([]interface{}, 0)
	sql := fmt.Sprintf("REPLACE INTO `%s` ", table)

	smap := list[0]
	keys := bytes.NewBufferString("")

	keysIndex := []string{}

	for k, _ := range smap {
		keysIndex = append(keysIndex, k)
		keys.WriteString(fmt.Sprintf("`%s`,", k))
	}
	keysSplit := string(keys.Bytes()[0 : keys.Len()-1])

	sql += fmt.Sprintf("(%s) VALUES ", keysSplit)

	for _, smap := range list {
		vals := bytes.NewBufferString("")

		for i := 0; i < len(keysIndex); i++ {
			k := keysIndex[i]
			v := smap[k]
			vals.WriteString("?,")
			valList = append(valList, v)
		}

		valsSplit := string(vals.Bytes()[0 : vals.Len()-1])
		sql += fmt.Sprintf("(%s),", valsSplit)
	}

	sql = string([]byte(sql)[0 : len(sql)-1])

	return this.Exec(sql, valList...)

}

// =======================================================================================================
// -------------------------------------------- Constructor ----------------------------------------------
// =======================================================================================================

// 初始化数据库
// 此时已经打开连接池，如果数据库连接失败会返回error
func NewClient(protocol string, host string, port uint32, user string, password string, database string, ssl bool, rootCertData []byte) (*Client, error) {

	client := new(Client)

	client.Host = host
	client.Port = port
	client.User = user
	client.Password = password
	client.Database = database
	client.Protocol = protocol
	client.SSL = ssl
	client.RootCertData = rootCertData
	client.Config = NewClientDnsConfigure()

	client.maxConn = 0
	client.maxIdleConn = 0

	client.Exec = client.exec
	client.Query = client.query

	if err := client.connect(); err != nil {
		log.Println(&NetError{s: "connect error:" + err.Error()})
		return nil, err
	}

	return client, nil
}

// 初始化一个TCP客户端
func NewTcpClient(host string, port uint32, user string, password string, database string, ssl bool, rootCertData []byte) (*Client, error) {
	return NewClient("tcp", host, port, user, password, database, ssl, rootCertData)
}

// =======================================================================================================
// -------------------------------------------- Public Api -----------------------------------------------
// =======================================================================================================

// UPDATE `Table` SET `field_1` = Value  Where id = 1
// UPDATE SET `field_1` = ? WHERE id = ? ,Value,1
// 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档
// ?占位符是字符串安全的,请尽量使用?占位符
func (this *Client) exec(sqlFmt string, sqlValue ...interface{}) *ClientExecResult {

	result := new(ClientExecResult)

	if err := this.connect(); err != nil {
		result.Err = err
		return result
	}

	var ret sql.Result
	var err error

	ret, err = this.db.Exec(sqlFmt, sqlValue...)
	result.Result = ret

	result.Err = err

	if result.Err != nil && len(result.Err.Error()) == 0 {
		result.Err = &NetError{s: "empty error msg"}
	}

	if Debug && err != nil {
		log.Println("[Litedb Debug] exec error:", err, sqlFmt, sqlValue)
	}

	return result
}

// SELECT * FROM Table WHERE id = 1
// SELECT * FROM Table WHERE id = ?
// 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档
// ?占位符是字符串安全的,请尽量使用?占位符
func (this *Client) query(sqlFmt string, sqlValue ...interface{}) *ClientQueryResult {

	result := new(ClientQueryResult)

	if err := this.connect(); err != nil {
		result.Err = err
		return result
	}

	rows, err := this.db.Query(sqlFmt, sqlValue...)
	result.Rows = rows

	result.Err = err

	if result.Err != nil && len(result.Err.Error()) == 0 {
		result.Err = &NetError{s: "empty error msg"}
	}

	if Debug && err != nil {
		log.Println("[Litedb Debug] query error:", err, sqlFmt, sqlValue)
	}

	return result

}

func (this *Client) DBStats() sql.DBStats {
	return this.db.Stats()
}

func (this *Client) SetMaxIdleConn(n int) {
	this.maxIdleConn = n
	this.db.SetMaxIdleConns(n)
}

func (this *Client) SetConnMaxLifetime(d time.Duration) {
	this.connMaxLifetime = d
	this.db.SetConnMaxLifetime(d)
}

func (this *Client) SetMaxConn(n int) {
	this.maxConn = n
	this.db.SetMaxOpenConns(n)
}

// 关闭数据库
func (this *Client) Close() error {

	if this.db != nil {
		return this.db.Close()
	}

	return nil
}

// ping
func (this *Client) Ping() error {
	if err := this.connect(); err != nil {
		return &NetError{s: "ping error:" + err.Error()}
	}
	return this.db.Ping()
}

// 开启事务
func (this *Client) Begin() (*Transaction, error) {

	if this.db == nil {
		err := this.connect()
		if err != nil {
			return nil, err
		}
	}

	tx, err := this.db.Begin()

	if Debug && err != nil {
		log.Println("[Litedb Debug] begin transaction error:", err)
	}

	if err != nil {
		return nil, err
	}

	tran := new(Transaction)
	tran.tx = tx
	tran.db = this.db
	tran.Exec = tran.exec
	tran.Query = tran.query
	return tran, nil
}

// UPDATE `Table` SET `field_1` = Value  Where id = 1
// UPDATE SET `field_1` = ? WHERE id = ? ,Value,1
// 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档
// ?占位符是字符串安全的,请尽量使用?占位符
func (this *Transaction) exec(sqlFmt string, sqlValue ...interface{}) *ClientExecResult {

	result := new(ClientExecResult)
	var ret sql.Result
	var err error

	ret, err = this.tx.Exec(sqlFmt, sqlValue...)
	result.Result = ret
	result.Err = err
	if Debug && err != nil {
		log.Println("[Litedb Debug] exec transaction error:", err, sqlFmt, sqlValue)
	}
	return result
}

// SELECT * FROM Table WHERE id = 1
// SELECT * FROM Table WHERE id = ?
// 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档
// ?占位符是字符串安全的,请尽量使用?占位符
func (this *Transaction) query(sqlFmt string, sqlValue ...interface{}) *ClientQueryResult {

	result := new(ClientQueryResult)

	rows, err := this.tx.Query(sqlFmt, sqlValue...)
	result.Rows = rows
	result.Err = err
	if Debug && err != nil {
		log.Println("[Litedb Debug] query transaction error:", err, sqlFmt, sqlValue)
	}
	return result

}

// 提交事务
func (this *Transaction) Commit() error {

	err := this.tx.Commit()
	if Debug && err != nil {
		log.Println("[Litedb Debug] commit transaction error:", err)
	}
	return err
}

// 回滚事务
func (this *Transaction) Roolback() error {

	return this.Rollback()
}

// 回滚事务
func (this *Transaction) Rollback() error {

	err := this.tx.Rollback()
	if Debug && err != nil {
		log.Println("[Litedb Debug] rollback transaction error:", err)
	}
	return err
}

// =======================================================================================================
// -------------------------------------------- Private Api ----------------------------------------------
// =======================================================================================================

func (this *Client) connect() error {

	if this.db == nil {
		var err error

		if this.SSL {
			rootCertPool := x509.NewCertPool()
			if ok := rootCertPool.AppendCertsFromPEM(this.RootCertData); !ok {
				return errors.New("Failed to append PEM.")
			}
			sslerr := mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool})

			if sslerr != nil {
				return sslerr
			}
		}

		////mysql.RegisterTLSConfig("custom", &tls.Config{ RootCAs: rootCertPool,})
		//db, err := sql.Open("mysql", "user@tcp(localhost:3306)/test?tls=custom")

		this.db, err = sql.Open("mysql", this.parseDNS())
		if err != nil {
			if Debug {
				log.Println("[Litedb Debug] connection error:", err)
			}
			if this.db != nil {
				this.db.Close()
			}

			this.db = nil
			return err
		}
	}

	this.db.SetConnMaxLifetime(connMaxLiftTime)

	return nil

}

func (this *Client) parseDNS() string {

	config := ""

	if this.Config != nil {
		config = this.Config.Parse()
	}

	dns := ""

	if this.Protocol == "unix" {
		dns = fmt.Sprintf("%s:%s@%s(%s)/%s?%s", this.User, this.Password, this.Protocol, this.Host, this.Database, config)
	} else {
		dns = fmt.Sprintf("%s:%s@%s(%s:%d)/%s?%s", this.User, this.Password, this.Protocol, this.Host, this.Port, this.Database, config)

		if this.SSL {
			dns = fmt.Sprintf("%s&tls=custom", dns)
		}
	}

	if Debug {
		log.Println("[Litedb Debug] connection DNS:", dns)
	}

	return dns
}
