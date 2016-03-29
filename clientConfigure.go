package litedb

import (
	"bytes"
	"fmt"
)

//客户端DNS配置
type ClientDNSConfigure struct {
	configure map[string]string
}

func NewClientDnsConfigure() *ClientDNSConfigure{

	configure := new(ClientDNSConfigure)
	configure.configure = make(map[string]string,8)
	configure.defaultConfigure()
	return configure
}

// 设置一个客户端DNS设置.
// Set("timeout","5")
// 详细信息请参考golang mysql DNS语法
func (this *ClientDNSConfigure)Set(k ,v string) bool {

	this.configure[k] = v
	return true

}

// 移除设置
// Remove("timeout")
func (this *ClientDNSConfigure)Remove(k string) bool {

	delete(this.configure, k)
	return true

}

// 将起解析DNS格式的字符串
func (this *ClientDNSConfigure)Parse() string {

	buffer := bytes.NewBufferString("")

	for index,item := range this.configure {
		buffer.WriteString(fmt.Sprintf("%s=%s&",index,item))
	}

	return string(buffer.Bytes()[0:buffer.Len() - 1])
}

func (this *ClientDNSConfigure)defaultConfigure(){

	this.Set("charset","utf8mb4")
	this.Set("timeout","5s")			//connect timeout
	this.Set("readTimeout","15s")
	this.Set("writeTimeout","15s")
	this.Set("strict","true")
	this.Set("sql_notes","false")
	this.Set("clientFoundRows","true")
	this.Set("collation","utf8mb4_general_ci")
}


