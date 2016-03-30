# litedb
--
    import "litedb"


## Usage

#### func  ListStructToMap

```go
func ListStructToMap(vs interface{}) ([]map[string]string, error)
```

#### func  StructToMap

```go
func StructToMap(structV interface{}) (map[string]string, error)
```

#### func  ToInt64

```go
func ToInt64(value interface{}) (d int64)
```
ToInt64 interface to int64

#### func  ToStr

```go
func ToStr(value interface{}, args ...int) (s string)
```
ToStr interface to string

#### type Client

```go
type Client struct {
	Sql
	Host     string
	Port     uint32
	User     string
	Password string
	Database string
	Protocol string
	Config   *ClientDNSConfigure
}
```

客户端

#### func  NewClient

```go
func NewClient(protocol string, host string, port uint32, user string, password string, database string) *Client
```
初始化数据库 此时并未打开连接池 只有在真实需要与数据库交互的时候才会进行连接.

#### func  NewTcpClient

```go
func NewTcpClient(host string, port uint32, user string, password string, database string) *Client
```
初始化一个TCP客户端

#### func (*Client) Begin

```go
func (this *Client) Begin() (*Transaction, error)
```
开启事务

#### func (*Client) Close

```go
func (this *Client) Close() error
```
关闭数据库

#### func (*Client) Ping

```go
func (this *Client) Ping() error
```
ping

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
将起解析DNS格式的字符串

#### func (*ClientDNSConfigure) Remove

```go
func (this *ClientDNSConfigure) Remove(k string) bool
```
移除设置 Remove("timeout")

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

Client.Exec 的结果

#### type ClientQueryResult

```go
type ClientQueryResult struct {
	Rows *sql.Rows
	Err  error
}
```

Client.Query 的结果

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
ToMap 将结果集转换为Map类型. 这个操作不进行任何类型转换. 因为这里的类型转换需要一次SQL去反射字段类型. 更多的时候会得不偿失.

#### func (*ClientQueryResult) ToStruct

```go
func (this *ClientQueryResult) ToStruct(containers interface{}) error
```
将结果集转换成一个struct 数组 var containers []Person

ToStruct(&containers) 对于struct类型,支持以下字段类型: int8

int16

int32

int64

int

uint8

uint16

uint32

uint64

uint

float32

float64

string

[]byte

#### type MarshalBinary

```go
type MarshalBinary interface {
	MarshalDB() ([]byte, error)
}
```

支持struct中的字段拥有更复杂的类型. 需要实现该接口才能正确的打包成string插入数据库中

#### type Sql

```go
type Sql struct {
	Exec  func(sqlFmt string, sqlValue ...interface{}) *ClientExecResult
	Query func(sqlFmt string, sqlValue ...interface{}) *ClientQueryResult
}
```

Sql操作集

#### func (*Sql) BatchInsert

```go
func (this *Sql) BatchInsert(table string, vs interface{}) *ClientExecResult
```
批量插入 SQL语句为: REPLACE INTO `%s` (field,field) VALUES (?,?),(?,?) 我们为什么使用REPLACE
INTO 来支持批量插入. 使用Insert Into 的问题是全部待插入的数据行是事务一致的.因此,对于一次插入中,只要有行已经存在,则全部插入失败.

#### func (*Sql) Delete

```go
func (this *Sql) Delete(table string, whereFmt string, whereValue ...interface{}) *ClientExecResult
```
根据Where条件删除数据

#### func (*Sql) Insert

```go
func (this *Sql) Insert(table string, v interface{}) *ClientExecResult
```
对Struct类型的支持,使用 db tag 进行数据库字段映射 对Map类型会将value转换为string.请确保map类型中只包含基本数据类型

#### func (*Sql) InsertOrUpdate

```go
func (this *Sql) InsertOrUpdate(table string, v interface{}) *ClientExecResult
```
插入或更新行(当主键已存在的时候) SQL语句为: INSERT INTO .... ON DUPLICATE KEY UPDATE .... 全部字段更新

#### func (*Sql) InsertOrUpdateFields

```go
func (this *Sql) InsertOrUpdateFields(table string, v interface{}, updateFields ...string) *ClientExecResult
```
map类型无必要使用该方法 插入或更新行(当主键已存在的时候) SQL语句为: INSERT INTO .... ON DUPLICATE KEY UPDATE
.... 可以指定更新字段

#### func (*Sql) Update

```go
func (this *Sql) Update(table string, v interface{}, whereFmt string, whereValue ...interface{}) *ClientExecResult
```
对Struct类型的支持,使用 db tag 进行数据库字段映射 对Map类型会将value转换为string.请确保map类型中只包含基本数据类型 where
条件写法 id = ?

#### func (*Sql) UpdateFields

```go
func (this *Sql) UpdateFields(table string, v interface{}, fields []string, whereFmt string, whereValue ...interface{}) *ClientExecResult
```
map类型无必要使用该方法 部分字段更新
该接口的意义是struct类型为完整的数据库字段映射.但某些时候我们仅仅需要更新部分字段.此时,如果使用完整映射的进行更新操作 则更容易误覆盖.
因此提供了这个接口进行部分字段更新. fields 就是需要更新的数据库字段名称 v,whereFmt,WhereValue 等值意义不变

#### type StrTo

```go
type StrTo string
```

StrTo is the target string

#### func (StrTo) Bool

```go
func (f StrTo) Bool() (bool, error)
```
Bool string to bool

#### func (*StrTo) Clear

```go
func (f *StrTo) Clear()
```
Clear string

#### func (StrTo) Exist

```go
func (f StrTo) Exist() bool
```
Exist check string exist

#### func (StrTo) Float32

```go
func (f StrTo) Float32() (float32, error)
```
Float32 string to float32

#### func (StrTo) Float64

```go
func (f StrTo) Float64() (float64, error)
```
Float64 string to float64

#### func (StrTo) Int

```go
func (f StrTo) Int() (int, error)
```
Int string to int

#### func (StrTo) Int16

```go
func (f StrTo) Int16() (int16, error)
```
Int16 string to int16

#### func (StrTo) Int32

```go
func (f StrTo) Int32() (int32, error)
```
Int32 string to int32

#### func (StrTo) Int64

```go
func (f StrTo) Int64() (int64, error)
```
Int64 string to int64

#### func (StrTo) Int8

```go
func (f StrTo) Int8() (int8, error)
```
Int8 string to int8

#### func (*StrTo) Set

```go
func (f *StrTo) Set(v string)
```
Set string

#### func (StrTo) String

```go
func (f StrTo) String() string
```
String string to string

#### func (StrTo) Uint

```go
func (f StrTo) Uint() (uint, error)
```
Uint string to uint

#### func (StrTo) Uint16

```go
func (f StrTo) Uint16() (uint16, error)
```
Uint16 string to uint16

#### func (StrTo) Uint32

```go
func (f StrTo) Uint32() (uint32, error)
```
Uint32 string to uint31

#### func (StrTo) Uint64

```go
func (f StrTo) Uint64() (uint64, error)
```
Uint64 string to uint64

#### func (StrTo) Uint8

```go
func (f StrTo) Uint8() (uint8, error)
```
Uint8 string to uint8

#### type Transaction

```go
type Transaction struct {
	Sql
}
```

事务客户端

#### func (*Transaction) Commit

```go
func (this *Transaction) Commit() error
```
提交事务

#### func (*Transaction) Roolback

```go
func (this *Transaction) Roolback() error
```
回滚事务

#### type UnmarshalBinary

```go
type UnmarshalBinary interface {
	UnmarshalDB(data []byte) error
}
```

对 MarshalBinary 的反向操作
