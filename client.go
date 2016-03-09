package litedb

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"os"
	"errors"
	"bytes"
)


type Client struct {

	Host 		string
	Port 		uint32
	User 		string
	Password 	string
	Database 	string
	Protocol 	string
	Config 		*ClientDNSConfigure
	LastResult  *ClientResult
	db 			*sql.DB
	tx 			*sql.Tx
}

// =======================================================================================================
// -------------------------------------------- Constructor ----------------------------------------------
// =======================================================================================================

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

func NewTcpClient(host string,port uint32,user string,password string,database string) *Client{
	return NewClient("tcp",host,port,user,password,database)
}

// =======================================================================================================
// -------------------------------------------- Public Api -----------------------------------------------
// =======================================================================================================

func (this *Client)Exec(sqlFmt string,sqlValue...interface{}) (*ClientExecResult) {

	result := new(ClientExecResult)

	if err := this.connect();err != nil {
		result.Err = err
		return result
	}

	var ret sql.Result
	var err error
	//如果在事务中
	if this.tx != nil {
		ret,err =  this.tx.Exec(sqlFmt,sqlValue)
	}else{
		ret,err = this.db.Exec(sqlFmt,sqlValue...)
	}

	result.Result =ret
	result.Err = err
	return result
}

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

func (this *Client)Update(table string,v interface{},whereFmt string,whereValue...interface{})(*ClientExecResult) {

	smap,err := StructToMap(v)

	if err != nil {
		r :=  new(ClientExecResult)
		r.Err = errors.New("[LiteDB Update] " + err.Error())
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
	return this.Exec(sql,append(valList,whereValue))

}

func (this *Client)UpdateFields(table string,v interface{},fields []string,whereFmt string,whereValue...interface{}) (*ClientExecResult) {

	smap,err := StructToMap(v)

	if err != nil {
		r :=  new(ClientExecResult)
		r.Err = errors.New("[LiteDB Update] " + err.Error())
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
		set.WriteString(fmt.Sprintf("`%s`=?, ",k))
		valList = append(valList,v)
	}

	setSplit := string(set.Bytes()[0:set.Len() - 1])

	sql := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s",table,setSplit,whereFmt)
	return this.Exec(sql,append(valList,whereValue))


}

func (this *Client)Delete(table string,whereFmt string,whereValue...interface{})(*ClientExecResult) {
	sql := fmt.Sprintf("DELETE FROM `%s` WHERE %s",table,whereFmt)
	return this.Exec(sql,whereValue)
}

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

func (this *Client)Close() error{

	if this.db != nil {
		return this.db.Close()
	}

	return nil
}

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
	fmt.Fprintln(os.Stdout,dns)
	return dns
}




