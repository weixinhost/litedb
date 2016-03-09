### LiteDB

#### Intro

    LiteDB 的核心设计目标是提供一个轻量级的SQL封装.
    LiteDB 不会对设计范式与Mysql本身做更多的侵入
    LiteDB 提供基本的SQL CURD封装
    LiteDB 不提供任何形式的SQLBuilder
    LiteDB 进行延迟连接

#### Dev Plan

    1. 首先完成基本API的定型
    2. 进行大规模测试
    3. 保证跨协程级别安全
    4. 保证连接池的可用性
    5. 事务的支持



#### Init

```go

    host        := "127.0.0.1"
    port        := 3306
    user        := "root"
    password    := "root"
    database    := "database_name"

   client := litedb.NewTcpClient(host,port,user,password,database)

```

#### Configure

```go

    client.Config.Set("timeout","5")
    client.Config.Set("charset","utf8")
```

#### Query

```go

 fullSql := "SELECT * FROM `my_table` LIMIT 10"

 ret := client.Query(fullSql)

 sql := "SELECT * FROM `my_table` where id = ?"

 ret := client.Query(sql,1)


 if err.Err != nil {
    //errro

 }else{

      maps,err := ret.ToMap()   //存储到一个[]map[string]string对象中
      first,err := ret.FirstToMap() //将首行存储到一个map[string]string对象中

      type Temp struct {

        Id      int `db:"id"`
        Name    string `db:"name"`

      }

     var listData []Temp

     err := ret.ToStruct(&listData)             //将全部结果存储到结构体中,使用db:"mysql_field_name"的形式进行数据库字段映射

     var data Temp

     err := ret.FirstToStruct(&data)            //将首行存储到一个结构体中

 }


```

#### Insert

```go

 type Temp struct {

        Id      int `db:"id"`
        Name    string `db:"name"`

   }

   newData := &Temp{
     Name : "my name"
   }


    ret := client.Insert("table",newData)

    if ret.Err != nil {

    }

```


#### Update
```go

 type Temp struct {

        Id      int `db:"id"`
        Name    string `db:"name"`

   }

   newData := &Temp{
     Name : "my new name"
   }

    ret := client.Insert("table",newData,"id=?",1)

    if ret.Err != nil {

    }

```

#### Delete
```go

    ret := client.Delete("table","id=?",1)

    if ret.Err != nil {

    }

```


## litedb
--
    import "litedb"


### Usage

#### type Client

```go
type Client struct {
	Host     string
	Port     uint32
	User     string
	Password string
	Database string
	Protocol string
	Config   *ClientDNSConfigure
}
```


#### func  NewClient

```go
func NewClient(protocol string, host string, port uint32, user string, password string, database string) *Client
```
初始化数据库

#### func  NewTcpClient

```go
func NewTcpClient(host string, port uint32, user string, password string, database string) *Client
```
初始化一个TCP客户端

#### func (*Client) Close

```go
func (this *Client) Close() error
```
关闭数据库

#### func (*Client) Delete

```go
func (this *Client) Delete(table string, whereFmt string, whereValue ...interface{}) *ClientExecResult
```
根据Where条件删除数据

#### func (*Client) Exec

```go
func (this *Client) Exec(sqlFmt string, sqlValue ...interface{}) *ClientExecResult
```
UPDATE `Table` SET `field_1` = Value Where id = 1 UPDATE SET `field_1` = ? WHERE
id = ? ,Value,1 支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档 ?占位符是字符串安全的,请尽量使用?占位符

#### func (*Client) Insert

```go
func (this *Client) Insert(table string, v interface{}) *ClientExecResult
```
仅支持struct类型数据.需要通过 db tag来进行数据库字段映射

#### func (*Client) Ping

```go
func (this *Client) Ping() error
```
ping

#### func (*Client) Query

```go
func (this *Client) Query(sqlFmt string, sqlValue ...interface{}) *ClientQueryResult
```
SELECT * FROM Table WHERE id = 1 SELECT * FROM Table WHERE id = ?
支持完整的SQL语句与?占位符.对于?占位符的使用请参考官方文档 ?占位符是字符串安全的,请尽量使用?占位符

#### func (*Client) Update

```go
func (this *Client) Update(table string, v interface{}, whereFmt string, whereValue ...interface{}) *ClientExecResult
```
仅支持struct类型数据.需要通过 db tag来进行数据库字段映射 where 条件写法 id = ?

#### func (*Client) UpdateFields

```go
func (this *Client) UpdateFields(table string, v interface{}, fields []string, whereFmt string, whereValue ...interface{}) *ClientExecResult
```
部分字段更新 该接口的意义是struct类型为完整的数据库字段映射.但某些时候我们仅仅需要更新部分字段.此时,如果使用完整映射的进行更新操作 则更容易误覆盖.
因此提供了这个接口进行部分字段更新. fields 就是需要更新的数据库字段名称 v,whereFmt,WhereValue 等值意义不变

#### type ClientDNSConfigure

```go
type ClientDNSConfigure struct {
}
```

客户端DNS配置

#### func  NewClientDnsConfigure

```go
func NewClientDnsConfigure() *ClientDNSConfigure
```

#### func (*ClientDNSConfigure) Parse

```go
func (this *ClientDNSConfigure) Parse() string
```
将起解析成字符串

#### func (*ClientDNSConfigure) Remove

```go
func (this *ClientDNSConfigure) Remove(k string) bool
```

#### func (*ClientDNSConfigure) Set

```go
func (this *ClientDNSConfigure) Set(k, v string) bool
```
设置一个客户端DNS设置. Set("timeout","5") 详细信息请参考golang mysql DNS语法

#### type ClientExecResult

```go
type ClientExecResult struct {
	Result sql.Result
	Err    error
}
```

Exec 的结果

#### type ClientQueryResult

```go
type ClientQueryResult struct {
	Rows *sql.Rows
	Err  error
}
```

Query 的结果

#### func (*ClientQueryResult) FirstToMap

```go
func (this *ClientQueryResult) FirstToMap() (map[string]string, error)
```
将Rows中的首行解析成一个map[string]string

#### func (*ClientQueryResult) FirstToStruct

```go
func (this *ClientQueryResult) FirstToStruct(v interface{}) error
```
将首行解析成一个Struct ,需要传递一个 struct的指针. struct 定义中使用标签 tag 来进行数据库字段映射,比如 struct {

    	 Id int `db:"id"`
      Name string `db:"name"`

}

#### func (*ClientQueryResult) ToMap

```go
func (this *ClientQueryResult) ToMap() ([]map[string]string, error)
```
ToMap 将结果集转换为Map类型 这个操作不进行任何类型转换 因为这里的类型转换需要一次SQL去反射字段类型 更多的时候会得不偿失

#### func (*ClientQueryResult) ToStruct

```go
func (this *ClientQueryResult) ToStruct(containers interface{}) error
```
将结果集转换成一个struct 数组 var containers []Person ToStruct(&containers)




