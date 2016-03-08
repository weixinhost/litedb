package litedb

import (
	"bytes"
	"fmt"
)

type ClientDNSConfigure struct {
	configure map[string]string
}

func NewClientDnsConfigure() *ClientDNSConfigure{

	configure := new(ClientDNSConfigure)
	configure.configure = make(map[string]string,8)
	configure.defaultConfigure()
	return configure
}

func (this *ClientDNSConfigure)Set(k ,v string) bool {

	this.configure[k] = v
	return true

}

func (this *ClientDNSConfigure)Remove(k string) bool {

	delete(this.configure, k)
	return true

}

func (this *ClientDNSConfigure)Parse() string {

	buffer := bytes.NewBufferString("")

	for index,item := range this.configure {
		buffer.WriteString(fmt.Sprintf("%s=%s&",index,item))
	}

	return string(buffer.Bytes()[0:buffer.Len() - 1])
}


func (this *ClientDNSConfigure)defaultConfigure(){

	this.Set("charset","utf8")
	this.Set("timeout","5s")			//connect timeout
	this.Set("readTimeout","15s")
	this.Set("writeTimeout","15s")
	this.Set("strict","true")
	this.Set("sql_notes","false")
	this.Set("clientFoundRows","true")

}


