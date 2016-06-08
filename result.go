package litedb

import (
	"database/sql"
	"reflect"
)

// Client.Exec 的结果
type ClientExecResult struct {
	Result sql.Result
	Err    error //db error
	Warn   error // db warning
}

// Client.Query 的结果
type ClientQueryResult struct {
	Rows *sql.Rows
	Err  error // db error
	Warn error // db warning
}

//支持struct中的字段拥有更复杂的类型.
//需要实现该接口才能正确的打包成string插入数据库中
type MarshalBinary interface {
	MarshalDB() ([]byte, error)
}

//对 MarshalBinary 的反向操作
type UnmarshalBinary interface {
	UnmarshalDB(data []byte) error
}

//ToMap 将结果集转换为Map类型.
//这个操作不进行任何类型转换.
//因为这里的类型转换需要一次SQL去反射字段类型.
//更多的时候会得不偿失.
func (this *ClientQueryResult) ToMap() ([]map[string]string, error) {

	if this.Err != nil {
		return nil, &SQLError{s: this.Err.Error()}
	}
	defer func() {
		this.Rows.Close()
	}()

	fields, err := this.Rows.Columns()

	if err != nil {
		return nil, &SQLError{s: this.Err.Error()}
	}

	parsed := make([]map[string]string, 0)

	for this.Rows.Next() {

		scanStore := make([]interface{}, 0, len(fields))
		tempData := make(map[string]interface{}, len(fields))

		for _, field := range fields {
			var tmp []byte
			scanStore = append(scanStore, &tmp)
			tempData[field] = &tmp
		}

		err = this.Rows.Scan(scanStore...)

		if err != nil {
			return nil, &SQLError{s: this.Err.Error()}
		}

		var parsedTmp map[string]string = make(map[string]string, 0)

		for key, raw := range tempData {
			parsedTmp[key] = string(*(raw.(*[]byte)))
		}

		parsed = append(parsed, parsedTmp)
	}

	return parsed, nil
}

// 将Rows中的首行解析成一个map[string]string
func (this *ClientQueryResult) FirstToMap() (map[string]string, error) {

	maps, err := this.ToMap()

	if err != nil {
		return nil, err
	}

	if len(maps) > 0 {
		return maps[0], nil
	}

	return nil, &EmptyRowsError{}
}

// 将首行解析成一个Struct ,需要传递一个 struct的指针.
// struct 定义中使用标签 tag 来进行数据库字段映射,比如
// struct {
// 	 Id int `db:"id"`
//   Name string `db:"name"`
// }
func (this *ClientQueryResult) FirstToStruct(v interface{}) error {

	first, err := this.FirstToMap()

	if err != nil {
		return err
	}

	return mapToStruct(first, v)

}

//将结果集转换成一个struct 数组
// var containers []Person
//
// ToStruct(&containers)
// 对于struct类型,支持以下字段类型:
// int8
//
// int16
//
// int32
//
// int64
//
// int
//
// uint8
//
// uint16
//
// uint32
//
// uint64
//
// uint
//
// float32
//
// float64
//
// string
//
// []byte
func (this *ClientQueryResult) ToStruct(containers interface{}) error {

	maps, err := this.ToMap()

	if err != nil {
		return err
	}

	val := reflect.ValueOf(containers)
	typ := reflect.TypeOf(containers)

	if typ.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice {
		return &ReflectError{s: "unsuported reflect type:" + typ.Kind().String()}
	}

	etyp := typ.Elem()

	v := val.Elem()

	for _, item := range maps {

		nv := reflect.New(etyp.Elem())

		err := mapToReflect(item, etyp.Elem(), nv.Elem())

		if err != nil {
			return err
		}

		v.Set(reflect.Append(v, nv.Elem()))
	}

	return nil
}

func mapToStruct(mapV map[string]string, structV interface{}) error {

	t := reflect.TypeOf(structV).Elem()

	p := reflect.ValueOf(structV).Elem()

	if reflect.ValueOf(structV).IsNil() {
		return &ReflectError{s: "store struct is nil-pointer"}
	}

	return mapToReflect(mapV, t, p)
}

func mapToReflect(mapV map[string]string, t reflect.Type, p reflect.Value) error {

	if p.Kind() != reflect.Struct {
		return &ReflectError{s: "store value is non-struct."}
	}

	if t.NumField() < 1 {
		return &ReflectError{s: "store struct is empty."}
	}

	for i := 0; i < t.NumField(); i++ {

		field := t.Field(i)

		tag := field.Tag.Get("db")

		if len(tag) < 1 || tag == "-" {
			continue
		}

		if tv, ok := mapV[tag]; ok == true {

			var s StrTo
			s.Set(tv)

			fv := p.FieldByName(field.Name)

			if fv.IsValid() == false || fv.CanSet() == false {
				return &ReflectError{s: "filed:" + field.Name + " value error"}
			}

			var de error = nil

			switch filev := field.Type.Kind(); filev {

			case reflect.Uint8:
				{
					if dv, de := s.Uint8(); de == nil {
						fv.SetUint(uint64(dv))
					}
					break
				}
			case reflect.Uint16:
				{
					if dv, de := s.Uint16(); de == nil {
						fv.SetUint(uint64(dv))
					}
					break
				}
			case reflect.Uint32:
				{
					if dv, de := s.Uint32(); de == nil {
						fv.SetUint(uint64(dv))
					}
					break
				}
			case reflect.Uint64:
				{
					if dv, de := s.Uint64(); de == nil {
						fv.SetUint(uint64(dv))
					}
					break
				}
			case reflect.Uint:
				{
					if dv, de := s.Uint(); de == nil {
						fv.SetUint(uint64(dv))
					}
					break
				}

			case reflect.Int8:
				{
					if dv, de := s.Int8(); de == nil {
						fv.SetInt(int64(dv))
					}
					break
				}
			case reflect.Int16:
				{
					if dv, de := s.Int16(); de == nil {
						fv.SetInt(int64(dv))
					}
					break
				}
			case reflect.Int32:
				{
					if dv, de := s.Int32(); de == nil {
						fv.SetInt(int64(dv))
					}
					break
				}
			case reflect.Int64:
				{
					if dv, de := s.Int64(); de == nil {
						fv.SetInt(int64(dv))
					}
					break
				}
			case reflect.Int:
				{
					if dv, de := s.Int(); de == nil {
						fv.SetInt(int64(dv))
					}
					break
				}
			case reflect.Float32:
				{
					if dv, de := s.Float32(); de == nil {
						fv.SetFloat(float64(dv))
					}
					break
				}
			case reflect.Float64:
				{
					if dv, de := s.Float64(); de == nil {
						fv.SetFloat(float64(dv))
					}
					break
				}

			case reflect.String:
				{
					fv.SetString(s.String())
					break
				}

			default:
				{

					m := fv.MethodByName("UnmarshalDB")

					if m.IsValid() {
						if len(s.String()) > 0 {
							var setEle reflect.Value
							if fv.Type().Kind() == reflect.Ptr {
								setEle = reflect.New(fv.Type().Elem())
							} else {
								setEle = reflect.New(fv.Type())
							}

							nm := setEle.MethodByName("UnmarshalDB")

							vals := nm.Call([]reflect.Value{
								reflect.ValueOf([]byte(s.String())),
							})

							if len(vals) > 0 {
								errVal := vals[0]
								if !errVal.IsNil() {
									de = &ReflectError{"marshal error:" + errVal.Interface().(error).Error()}
								} else {
									fv.Set(setEle)
								}
							}
						}

					} else {
						de = &ReflectError{s: "unsuported type:" + filev.String()}
					}
				}
			}

			if de != nil {
				return de
			}
		}
	}

	return nil
}

func StructToMap(structV interface{}) (map[string]string, error) {

	t := reflect.TypeOf(structV)

	if t.Kind() == reflect.Map {

		retMap := make(map[string]string, 0)

		parsedStructV, ok := structV.(map[string]interface{})

		if !ok {
			return nil, &ReflectError{s: "unsupported this map type:" + t.String() + ", supported map[string]interface{} only."}
		}

		for k, v := range parsedStructV {
			retMap[k] = ToStr(v)
		}

		return retMap, nil
	}

	p := reflect.ValueOf(structV)

	if p.Kind() == reflect.Ptr {

		if reflect.ValueOf(structV).IsNil() {
			return nil, &ReflectError{s: "store struct is nil-pointer"}
		}

		t = t.Elem()
		p = p.Elem()
	}

	return reflectToMap(t, p)
}

func reflectToMap(t reflect.Type, p reflect.Value) (map[string]string, error) {

	if p.Kind() != reflect.Struct {
		return nil, &ReflectError{s: "struct is non-struct"}
	}

	if t.NumField() < 1 {
		return nil, &ReflectError{s: "store struct is empty"}
	}

	ret := make(map[string]string, 0)

	for i := 0; i < t.NumField(); i++ {

		field := t.Field(i)

		tag := field.Tag.Get("db")

		if len(tag) < 1 || tag == "-" {
			continue
		}

		fv := p.FieldByName(field.Name)

		var storStr string = ""

		if fv.Type().Kind() == reflect.Ptr && fv.IsNil() {
			//skip nil pointer
		} else {

			m := fv.MethodByName("MarshalDB")
			if m.IsValid() {
				vals := m.Call([]reflect.Value{})
				if len(vals) > 0 {
					dataVal := vals[0]
					errVal := vals[1]
					if errVal.IsNil() && dataVal.CanInterface() {
						data := dataVal.Interface().([]byte)
						storStr = string(data)
					} else {
						return nil, &ReflectError{s: "MarshalDB error:" + errVal.Interface().(error).Error()}
					}
				}
			} else {
				storStr = ToStr(fv.Interface())
			}
		}

		ret[tag] = storStr
	}

	return ret, nil

}

func ListStructToMap(vs interface{}) ([]map[string]string, error) {

	ret := make([]map[string]string, 0)

	t := reflect.TypeOf(vs)
	p := reflect.ValueOf(vs)

	if p.Kind() == reflect.Ptr {
		t = t.Elem()
		p = p.Elem()
	}

	kind := p.Kind()

	if kind != reflect.Array && kind != reflect.Slice {
		return ret, &ReflectError{s: " interface{} is non-array or non-slice"}
	}

	len := p.Len()

	if len < 1 {
		return ret, &ReflectError{s: "interface{} is empty"}
	}

	for i := 0; i < len; i++ {
		v := p.Index(i)

		rv, re := reflectToMap(v.Type(), v)
		if re != nil {
			return ret, &ReflectError{s: "error:" + re.Error()}
		}
		ret = append(ret, rv)
	}

	return ret, nil
}
