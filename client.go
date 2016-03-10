package litedb

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"errors"
	"bytes"
)

// Mysql数据库客户端
// 底层使用 database/sql 以及 github.com/go-sql-driver/mysql 实现
// LiteDB 的设计目标为轻量的数据库操作封装.无意于任何的复杂关系映射.
// 仅仅以一种舒服的姿势进行数据库与程序对象的映射.
// 并且对基本的操作提供基本的语法糖,方便操作.

type Client struct {

	Host 		string
	Port 		uint32
	User 		string
	Password 	string
	Database 	string
	Protocol 	string
	Config 		*ClientDNSConfigure
	db 			*sql.DB
}

// =======================================================================================================
// -------------------------------------------- Constructor ----------------------------------------------
// =======================================================================================================

//初始化数据库
func NewClient(protocol string,host string,port uint32,user string,password string,database string) *Client {

	client := new(Client)

	client.Host 	= host
	client.Port 	= port
	client.User 	= user
	client.Password = password
	client.Database	= database
	client.Protocol = protocol
	client.Config 	= NewClientDnsConfigure()
	return client
}

//初始化一个TCP客户端
func NewTcpClient(host string,port uint32,user string,password string,database string) *Client{
	return NewClient("tcp",host,port,user,password,database)
}


// =======================================================================================================
// -------------------------------------------- Public Api -----------------------------------------------
// =======================================================================================================



// UPDATE `Table` SET `field_1` = Value  Where id = 1
// UPDATE SET `field_1` = ? WHERE id = ? ,Value,1
// 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档
// ?占位符是字符串安全的,请尽量使用?占位符
func (this *Client)Exec(sqlFmt string,sqlValue...interface{}) (*ClientExecResult) {

	result := new(ClientExecResult)

	if err := this.connect();err != nil {
		result.Err = err
		return result
	}

	var ret sql.Result
	var err error

	ret,err = this.db.Exec(sqlFmt,sqlValue...)

	result.Result =ret
	result.Err = err
	return result
}

// SELECT * FROM Table WHERE id = 1
// SELECT * FROM Table WHERE id = ?
// 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档
// ?占位符是字符串安全的,请尽量使用?占位符
func (this *Client)Query(sqlFmt string,sqlValue...interface{}) (*ClientQueryResult){

	result := new(ClientQueryResult)

	if err := this.connect();err != nil {
		result.Err = err
		return result
	}

	rows,err := this.db.Query(sqlFmt,sqlValue...)
	result.Rows = rows
	result.Err = err

	return result

}

// 仅支持struct类型数据.需要通过 db tag来进行数据库字段映射
func (this *Client)Insert(table string,v interface{})(*ClientExecResult) {

	smap,err := StructToMap(v)
	r :=  new(ClientExecResult)

	if err != nil {
		r.Err = errors.New("[LiteDB Insert] " + err.Error())
		return r
	}

	if len(smap) < 1 {
		r.Err = errors.New("[LiteDB Insert] Nothing Insert")
		return r

	}

	keys := bytes.NewBufferString("")
	vals := bytes.NewBufferString("")

	valList := make([]interface{},0)

	for k,v := range smap {
		keys.WriteString(fmt.Sprintf("`%s`,",k))
		vals.WriteString("?,")
		valList = append(valList,v)
	}

	keysSplit := string(keys.Bytes()[0:keys.Len() - 1])
	valsSplit := string(vals.Bytes()[0:vals.Len() -1])
	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s);",table,keysSplit,valsSplit)
	return this.Exec(sql,valList...)
}

// 仅支持struct类型数据.需要通过 db tag来进行数据库字段映射
// where 条件写法 id = ?
func (this *Client)Update(table string,v interface{},whereFmt string,whereValue...interface{})(*ClientExecResult) {

	smap,err := StructToMap(v)
	r :=  new(ClientExecResult)

	if err != nil {
		r.Err = errors.New("[LiteDB Update] " + err.Error())
		return r
	}

	if len(smap) < 1 {
		r.Err = errors.New("[LiteDB Update] Nothing Update")
		return r
	}

	set := bytes.NewBufferString("")
	valList := make([]interface{},0)

	for k,v := range smap {
		set.WriteString(fmt.Sprintf("`%s`=?, ",k))
		valList = append(valList,v)
	}

	setSplit := string(set.Bytes()[0:set.Len() - 1])

	sql := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s",table,setSplit,whereFmt)
	valList = append(valList,whereValue)
	return this.Exec(sql,valList...)

}

// 部分字段更新
// 该接口的意义是struct类型为完整的数据库字段映射.但某些时候我们仅仅需要更新部分字段.此时,如果使用完整映射的进行更新操作
// 则更容易误覆盖.
// 因此提供了这个接口进行部分字段更新.
// fields 就是需要更新的数据库字段名称
// v,whereFmt,WhereValue 等值意义不变
func (this *Client)UpdateFields(table string,v interface{},fields []string,whereFmt string,whereValue...interface{}) (*ClientExecResult) {

	smap,err := StructToMap(v)
	r :=  new(ClientExecResult)

	if err != nil {
		r.Err = errors.New("[LiteDB UpdateFields] " + err.Error())
		return r
	}

	if len(smap) < 1 {
		r.Err = errors.New("[LiteDB UpdateFields] Nothing Update")
		return r
	}

	vmap := make(map[string]string,0)

	for _,f := range fields {
		v,ok := smap[f]
		if ok == true {
			vmap[f] = v
		}
	}

	smap = vmap

	set := bytes.NewBufferString("")
	valList := make([]interface{},0)

	for k,v := range smap {
		set.WriteString(fmt.Sprintf("`%s`=?,",k))
		valList = append(valList,v)
	}

	setSplit := string(set.Bytes()[0:set.Len() - 1])

	sql := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s",table,setSplit,whereFmt)
	return this.Exec(sql,append(valList,whereValue...)...)


}

// 根据Where条件删除数据
func (this *Client)Delete(table string,whereFmt string,whereValue...interface{})(*ClientExecResult) {
	sql := fmt.Sprintf("DELETE FROM `%s` WHERE %s",table,whereFmt)
	return this.Exec(sql,whereValue)
}


// 插入或更新行(当主键已存在的时候)
// SQL语句为: INSERT INTO .... ON DUPLICATE KEY UPDATE ....
// 全部字段更新
func (this *Client)InsertOrUpdate(table string,v interface{}) (*ClientExecResult) {

	smap,err := StructToMap(v)
	r :=  new(ClientExecResult)

	if err != nil {
		r.Err = errors.New("[LiteDB InsertOrUpdate] " + err.Error())
		return r
	}

	if len(smap) < 1 {
		r.Err = errors.New("[LiteDB InsertOrUpdate] Nothing Insert")
		return r

	}

	insertKeys := bytes.NewBufferString("")
	insertVals := bytes.NewBufferString("")

	set := bytes.NewBufferString("")

	insertValList := make([]interface{},0)
	updateValList := make([]interface{},0)

	for k,v := range smap {
		insertKeys.WriteString(fmt.Sprintf("`%s`,",k))
		insertVals.WriteString("?,")

		set.WriteString(fmt.Sprintf("`%s`=?,",k))
		insertValList = append(insertValList,v)
		updateValList = append(updateValList,v)
	}

	keysSplit := string(insertKeys.Bytes()[0:insertKeys.Len() - 1])
	valsSplit := string(insertVals.Bytes()[0:insertVals.Len() -1])
	setSplit  := string(set.Bytes()[0:set.Len() -1])

	insertValList = append(insertValList,updateValList...)

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE  %s",table,keysSplit,valsSplit,setSplit)

	return this.Exec(sql,insertValList...)

}

// 插入或更新行(当主键已存在的时候)
// SQL语句为: INSERT INTO .... ON DUPLICATE KEY UPDATE ....
func (this *Client)InsertOrUpdateFields(table string,v interface{},updateFields...string)(*ClientExecResult){

	smap,err := StructToMap(v)
	r :=  new(ClientExecResult)

	if err != nil {
		r.Err = errors.New("[LiteDB InsertOrUpdateFields] " + err.Error())
		return r
	}

	if len(smap) < 1 {
		r.Err = errors.New("[LiteDB InsertOrUpdateFields] Nothing Insert")
		return r
	}

	if len(updateFields) < 1 {
		r.Err = errors.New("[LiteDB InsertOrUpdateFields] Nothing Update")
		return r
	}

	updateMap := make(map[string]string,0)
	for _,f := range updateFields {
		if v,ok := smap[f];ok {
			updateMap[f] = v
		}
	}

	if len(updateMap) < 1 {
		r.Err = errors.New("[LiteDB InsertOrUpdateFields] Nothing Update")
		return r
	}

	insertKeys := bytes.NewBufferString("")
	insertVals := bytes.NewBufferString("")

	set := bytes.NewBufferString("")

	insertValList := make([]interface{},0)
	updateValList := make([]interface{},0)

	for k,v := range smap {
		insertKeys.WriteString(fmt.Sprintf("`%s`,",k))
		insertVals.WriteString("?,")
		insertValList = append(insertValList,v)

	}

	for k,v := range updateMap {
		set.WriteString(fmt.Sprintf("`%s`=?,",k))
		updateValList = append(updateValList,v)
	}

	keysSplit := string(insertKeys.Bytes()[0:insertKeys.Len() - 1])
	valsSplit := string(insertVals.Bytes()[0:insertVals.Len() -1])
	setSplit  := string(set.Bytes()[0:set.Len() -1])

	insertValList = append(insertValList,updateValList...)

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE  %s",table,keysSplit,valsSplit,setSplit)

	return this.Exec(sql,insertValList...)


}

/*
func (this *Client)Begin() error{
	tx,err := this.db.Begin()
	if err != nil {
		return errors.New("[LiteDB Begin] " + err.Error())
	}
	this.tx = tx
	return nil
}

func (this *Client)Commit() error{

	if this.tx == nil {
		return errors.New("[LiteDB Commit] Transaction Not Begin.")
	}

	err := this.tx.Commit()
	this.tx = nil
	return err
}

func (this *Client)Rollback() error{

	if this.tx == nil {
		return errors.New("[LiteDB Commit] Transaction Not Begin.")
	}

	err := this.tx.Rollback()
	this.tx = nil
	return err
}
*/

//关闭数据库
func (this *Client)Close() error{

	if this.db != nil {
		return this.db.Close()
	}

	return nil
}

//ping
func (this *Client)Ping() error{
	if err := this.connect();err != nil {
		return errors.New("[LiteDB Ping] " + err.Error())
	}
	return this.db.Ping()
}

// =======================================================================================================
// -------------------------------------------- Private Api ----------------------------------------------
// =======================================================================================================

func (this *Client)connect() error{

	if this.db == nil {

		var err error = nil

		this.db,err = sql.Open("mysql",this.parseDNS())

		if 	err 	!= nil {
			this.db  = nil
			return err
		}

	}

	return nil

}

func(this *Client)parseDNS() string{

	config := ""

	if this.Config != nil {
		config = this.Config.Parse()
	}
	dns := fmt.Sprintf("%s:%s@%s(%s:%d)/%s?%s",this.User,this.Password,this.Protocol,this.Host,this.Port,this.Database,config)
	return dns
}




