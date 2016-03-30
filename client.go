// Mysql数据库客户端
// 底层使用 database/sql 以及 github.com/go-sql-driver/mysql 实现
// LiteDB 的设计目标为轻量的数据库操作封装.无意于任何的复杂关系映射.
// 仅仅以一种舒服的姿势进行数据库与程序对象的映射.
// 并且对基本的操作提供基本的语法糖,方便操作.

package litedb

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"errors"
	"bytes"

)


//Sql操作集
type Sql struct {
	Exec 	func(sqlFmt string,sqlValue...interface{}) (*ClientExecResult)
	Query   func(sqlFmt string,sqlValue...interface{}) (*ClientQueryResult)
}


//客户端
type Client struct {
	Sql
	Host 		string
	Port 		uint32
	User 		string
	Password 	string
	Database 	string
	Protocol 	string
	Config 		*ClientDNSConfigure
	db 			*sql.DB
	autoExec 	bool
}

//事务客户端
type Transaction struct {
	Sql
	tx *sql.Tx
	db *sql.DB
}


// 对Struct类型的支持,使用 db tag 进行数据库字段映射
// 对Map类型会将value转换为string.请确保map类型中只包含基本数据类型
func (this *Sql)Insert(table string,v interface{})(*ClientExecResult) {

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

// 对Struct类型的支持,使用 db tag 进行数据库字段映射
// 对Map类型会将value转换为string.请确保map类型中只包含基本数据类型
// where 条件写法 id = ?
func (this *Sql)Update(table string,v interface{},whereFmt string,whereValue...interface{})(*ClientExecResult) {

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
		set.WriteString(fmt.Sprintf("`%s`=?,",k))
		valList = append(valList,v)
	}
	setSplit := string(set.Bytes()[0:set.Len() - 1])
	sql := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s",table,setSplit,whereFmt)
	valList = append(valList,whereValue...)
	return this.Exec(sql,valList...)

}

// map类型无必要使用该方法
// 部分字段更新
// 该接口的意义是struct类型为完整的数据库字段映射.但某些时候我们仅仅需要更新部分字段.此时,如果使用完整映射的进行更新操作
// 则更容易误覆盖.
// 因此提供了这个接口进行部分字段更新.
// fields 就是需要更新的数据库字段名称
// v,whereFmt,WhereValue 等值意义不变
func (this *Sql)UpdateFields(table string,v interface{},fields []string,whereFmt string,whereValue...interface{}) (*ClientExecResult) {

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
func (this *Sql)Delete(table string,whereFmt string,whereValue...interface{})(*ClientExecResult) {
	sql := fmt.Sprintf("DELETE FROM `%s` WHERE %s",table,whereFmt)
	return this.Exec(sql,whereValue...)
}


// 插入或更新行(当主键已存在的时候)
// SQL语句为: INSERT INTO .... ON DUPLICATE KEY UPDATE ....
// 全部字段更新
func (this *Sql)InsertOrUpdate(table string,v interface{}) (*ClientExecResult) {

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

// map类型无必要使用该方法
// 插入或更新行(当主键已存在的时候)
// SQL语句为: INSERT INTO .... ON DUPLICATE KEY UPDATE ....
// 可以指定更新字段
func (this *Sql)InsertOrUpdateFields(table string,v interface{},updateFields...string)(*ClientExecResult){

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

// 批量插入
// SQL语句为: REPLACE INTO `%s` (field,field) VALUES (?,?),(?,?)
// 我们为什么使用REPLACE INTO 来支持批量插入.
// 使用Insert Into 的问题是全部待插入的数据行是事务一致的.因此,对于一次插入中,只要有行已经存在,则全部插入失败.
func (this *Sql)BatchInsert(table string,vs interface{}) (*ClientExecResult) {

	r := new(ClientExecResult)

	list,err := ListStructToMap(vs)

	if err != nil {
		r.Err = errors.New("[LiteDB BatchInsert] " + err.Error())
		return r
	}
	valList := make([]interface{},0)
	sql := fmt.Sprintf("REPLACE INTO `%s` ",table)

	smap := list[0]
	keys := bytes.NewBufferString("")

	keysIndex := []string{}

	for k,_ := range smap {
		keysIndex = append(keysIndex,k)
		keys.WriteString(fmt.Sprintf("`%s`,",k))
	}
	keysSplit := string(keys.Bytes()[0:keys.Len() - 1])

	sql += fmt.Sprintf("(%s) VALUES ",keysSplit)

	for _,smap := range list {
		vals  := bytes.NewBufferString("")

		for i:=0 ;i<len(keysIndex);i++ {
			k := keysIndex[i]
			v := smap[k]
			vals.WriteString("?,")
			valList = append(valList,v)
		}

		valsSplit := string(vals.Bytes()[0:vals.Len() -1])
		sql += fmt.Sprintf("(%s),",valsSplit)
	}

	sql = string([]byte(sql)[0:len(sql) -1])

	return this.Exec(sql,valList...)

}


// =======================================================================================================
// -------------------------------------------- Constructor ----------------------------------------------
// =======================================================================================================

//初始化数据库
//此时并未打开连接池
//只有在真实需要与数据库交互的时候才会进行连接.
func NewClient(protocol string,host string,port uint32,user string,password string,database string) *Client {

	client := new(Client)

	client.Host 	= host
	client.Port 	= port
	client.User 	= user
	client.Password = password
	client.Database	= database
	client.Protocol = protocol
	client.Config 	= NewClientDnsConfigure()

	client.Exec = client.exec
	client.Query =client.query

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
func (this *Client)exec(sqlFmt string,sqlValue...interface{}) (*ClientExecResult) {

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
func (this *Client)query(sqlFmt string,sqlValue...interface{}) (*ClientQueryResult){

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


//开启事务
func (this *Client)Begin()(*Transaction,error){

	tx,err := this.db.Begin()

	if err != nil {
		return nil,err
	}

	tran := new(Transaction)
	tran.tx = tx
	tran.db = this.db
	tran.Exec = tran.exec
	tran.Query = tran.query
	return tran,nil
}



// UPDATE `Table` SET `field_1` = Value  Where id = 1
// UPDATE SET `field_1` = ? WHERE id = ? ,Value,1
// 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档
// ?占位符是字符串安全的,请尽量使用?占位符
func (this *Transaction)exec(sqlFmt string,sqlValue...interface{}) (*ClientExecResult) {

	result := new(ClientExecResult)
	var ret sql.Result
	var err error

	ret,err = this.tx.Exec(sqlFmt,sqlValue...)
	result.Result = ret
	result.Err 	  = err
	return result
}

// SELECT * FROM Table WHERE id = 1
// SELECT * FROM Table WHERE id = ?
// 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档
// ?占位符是字符串安全的,请尽量使用?占位符
func (this *Transaction)query(sqlFmt string,sqlValue...interface{}) (*ClientQueryResult){

	result := new(ClientQueryResult)

	rows,err := this.tx.Query(sqlFmt,sqlValue...)
	result.Rows = rows
	result.Err = err

	return result

}


//提交事务
func (this *Transaction)Commit() error {

	return this.tx.Commit()
}

//回滚事务
func (this *Transaction)Roolback() error {

	return this.tx.Rollback()
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




