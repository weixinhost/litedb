package litedb

import (
	"database/sql"
	"errors"
	"reflect"
)

type ClientResult struct {

}

type ClientExecResult struct {

	Result sql.Result
	Err 	error
}

type ClientQueryResult struct {

	Rows *sql.Rows
	Err error

}

//ToMap 将结果集转换为Map类型
//这个操作不进行任何类型转换
//因为这里的类型转换需要一次SQL去反射字段类型
//更多的时候会得不偿失

func (this *ClientQueryResult)ToMap() ([]map[string]string,error)  {

	if this.Err != nil {
		return nil,errors.New("[LiteDB ToMap] " + this.Err.Error())
	}

	fields,err := this.Rows.Columns()

	if err != nil {
		return nil,errors.New("[LiteDB ToMap] " + err.Error())
	}

	parsed := make([]map[string]string,0)

	for this.Rows.Next() {

		scanStore := make([]interface{},0,len(fields))
		tempData := make(map[string]interface{},len(fields))

		for _,field := range fields {
			var tmp []byte
			scanStore = append(scanStore,&tmp)
			tempData[field] =&tmp
		}

		err = this.Rows.Scan(scanStore...)

		if err != nil {
			return nil,errors.New("[LiteDB ToMap] " + err.Error())
		}

		var parsedTmp map[string]string = make(map[string]string,0)

		for key,raw := range tempData {
			parsedTmp[key] = string(*(raw.(*[]byte)))
		}

		parsed = append(parsed,parsedTmp)
	}

	return parsed,nil
}

func (this *ClientQueryResult)FirstToMap() (map[string]string,error) {

	maps,err := this.ToMap()

	if err != nil {
		return nil,err
	}

	if len(maps) > 0 {
		return maps[0],nil
	}

	return nil,errors.New("[LiteDB FirstMap] RowNotFound")
}

func (this *ClientQueryResult)FirstToStruct(v interface{}) error {

	first,err := this.FirstToMap()

	if err != nil {
		return err
	}

	return mapToStruct(first,v)

}

func (this *ClientQueryResult)ToStruct(containers interface{}) error {

	maps,err := this.ToMap()

	if err != nil {
		return err
	}

	val := reflect.ValueOf(containers)
	typ := reflect.TypeOf(containers)

	if typ.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Slice{
		return errors.New("[LiteDB ToStruct] Unsupprted reflect type:" + typ.Kind().String())
	}

	etyp := typ.Elem()

	v := val.Elem()

	for _,item := range maps {

		nv := reflect.New(etyp.Elem())

		err := mapToReflect(item,etyp.Elem(),nv.Elem())

		if err != nil {
			return err
		}

		v.Set(reflect.Append(v,nv.Elem()))
	}

	return nil
}

func mapToStruct(mapV map[string]string,structV interface{}) error {

	t := reflect.TypeOf(structV).Elem()

	p := reflect.ValueOf(structV).Elem()

	if reflect.ValueOf(structV).IsNil() {
		return errors.New("[LiteDB FirstToStruct] store struct is nil")
	}

	return mapToReflect(mapV,t,p)
}

func mapToReflect(mapV map[string]string,t reflect.Type,p reflect.Value) error{

	if p.Kind() != reflect.Struct {
		return errors.New("[LiteDB FirstToStruct] struct is non-struct.")
	}



	if  t.NumField() < 1{

		return errors.New("[LiteDB FirstToStruct] store struct is empty.")
	}

	for i := 0;i < t.NumField();i++ {

		field := t.Field(i)

		tag := field.Tag.Get("db")

		if len(tag) < 1 || tag == "-" {
			continue;
		}

		if tv,ok :=mapV[tag];ok == true {

			var s StrTo
			s.Set(tv)

			fv := p.FieldByName(field.Name)

			if fv.IsValid() == false || fv.CanSet() == false {
				return errors.New("[LiteDB FirstToStruct] field:" + field.Name + " valid error")
			}

			var de error = nil

			switch filev := field.Type.Kind();filev {

			case reflect.Uint8: {
				if dv,de := s.Uint8();de == nil {
					fv.SetUint(uint64(dv))
				}
				break
			}
			case reflect.Uint16: {
				if dv,de := s.Uint16();de == nil {
					fv.SetUint(uint64(dv))
				}
				break
			}
			case reflect.Uint32: {
				if dv,de := s.Uint32();de == nil {
					fv.SetUint(uint64(dv))
				}
				break
			}
			case reflect.Uint64: {
				if dv,de := s.Uint64();de == nil {
					fv.SetUint(uint64(dv))
				}
				break
			}
			case reflect.Uint: {
				if dv,de := s.Uint();de == nil {
					fv.SetUint(uint64(dv))
				}
				break
			}

			case reflect.Int8: {
				if dv,de := s.Int8();de == nil {
					fv.SetInt(int64(dv))
				}
				break
			}
			case reflect.Int16: {
				if dv,de := s.Int16();de == nil {
					fv.SetInt(int64(dv))
				}
				break
			}
			case reflect.Int32: {
				if dv,de := s.Int32();de == nil {
					fv.SetInt(int64(dv))
				}
				break
			}
			case reflect.Int64: {
				if dv,de := s.Int64();de == nil {
					fv.SetInt(int64(dv))
				}
				break
			}
			case reflect.Int: {
				if dv,de := s.Int();de == nil {
					fv.SetInt(int64(dv))
				}
				break
			}
			case reflect.Float32 : {
				if dv,de := s.Float32();de == nil {
					fv.SetFloat(float64(dv))
				}
				break
			}
			case reflect.Float64: {
				if dv,de := s.Float64();de == nil {
					fv.SetFloat(float64(dv))
				}
				break
			}

			case reflect.String : {
				fv.SetString(s.String())
				break
			}

			default : {
				de = errors.New("[LiteDB mapToStruct] Unsupprted Type:" + filev.String())
			}
			}

			if de != nil {
				return de
			}
		}
	}

	return nil
}

func StructToMap(structV interface{}) (map[string]string,error){

	t := reflect.TypeOf(structV).Elem()

	p := reflect.ValueOf(structV).Elem()

	if reflect.ValueOf(structV).IsNil() {
		return nil,errors.New("[LiteDB structToMap] store struct is nil")
	}
	
	if p.Kind() == reflect.Map {
		return structV.(map[string]string),nil
	}

	if p.Kind() != reflect.Struct {
		return nil,errors.New("[LiteDB structToMap] struct is non-struct.")
	}



	if  t.NumField() < 1{

		return nil,errors.New("[LiteDB structToMap] store struct is empty.")
	}

	ret := make(map[string]string,0)

	for i := 0;i < t.NumField();i++ {

		field := t.Field(i)

		tag := field.Tag.Get("db")

		if len(tag) < 1 || tag == "-" {
			continue;
		}

		fv := p.FieldByName(field.Name)

		str := ToStr(fv.Interface())

		ret[tag] = str
	}

	return ret,nil
}



