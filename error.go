package litedb

//EmptyRowsError 未发现行
type EmptyRowsError struct {
}

func (err *EmptyRowsError) Error() string {
	return "[litedb] Rows Not Found"
}

//NetError 网络错误
type NetError struct {
	s string
}

func (err *NetError) Error() string {

	return "[litedb] Network Error:" + err.s

}

//SQLError 错误
type SQLError struct {
	s string
}

func (err *SQLError) Error() string {

	return "[litedb] SQL Error:" + err.s
}

//ReflectError 反射阶段错误
type ReflectError struct {
	s string
}

func (err *ReflectError) Error() string {

	return "[litedb] Reflect Error:" + err.s
}
