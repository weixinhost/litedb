### LiteDB

    一个轻便以及追求SQL性能的MYSQL客户端

#### Intro

    LiteDB 的核心设计目标是提供一个轻量级的SQL封装.
    LiteDB 不会对设计范式与Mysql本身做更多的侵入
    LiteDB 提供基本的SQL CURD封装
    LiteDB 不提供任何形式的SQLBuilder
    LiteDB 使用 `database/sql` 和 mysql驱动


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