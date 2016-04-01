package utils

import (
	"reflect"
	"fmt"
)



func ParseWhereMap(wheres interface{}) (string,[]interface{}){

	whereMap := make(map[string]interface{},0)
	valList := make([]interface{},0)

	if reflect.ValueOf(wheres).IsValid() {
		if reflect.TypeOf(wheres).Kind() == reflect.Map {
			whereMap = wheres.(map[string]interface{})
		}
	}

	where := "1 "

	for k,v := range whereMap {
		if reflect.TypeOf(v).Kind() != reflect.Map {
			where = where + fmt.Sprintf(" AND `%s` = ? ",k)
			valList = append(valList,v)
		}
	}

	for k,v := range whereMap {

		if reflect.TypeOf(v).Kind() == reflect.Map {

			vi := v.(map[string]interface{})

			t,ok1 := vi["type"]

			var vv interface{}

			ev,ok2 := vi["value"]

			if ok2 {
				if reflect.TypeOf(ev).Kind() == reflect.Struct {
					ep := reflect.ValueOf(ev)
					if ep.IsValid() {
						sn := ep.MethodByName("String")
						if sn.IsValid() {
							rets := sn.Call([]reflect.Value{})
							if len(rets) > 0 {
								strRet := rets[0]
								vv = strRet.Interface().(string)
							}
						}
					}
				}else{
					vv = ev
				}
			}
			
			if ok1 && ok2 {

				switch t.(string) {

				case "=" 		: {

					where  = where + fmt.Sprintf(" AND `%s` = ? ",k)
					valList = append(valList,vv)

					break}

				case ">" 		: {

					where  = where + fmt.Sprintf(" AND `%s` > ? ",k)
					valList = append(valList,vv)

					break}
				case "<" 		: {

					where  = where + fmt.Sprintf(" AND `%s` < ? ",k)
					valList = append(valList,vv)

					break}
				case "not in"	: {

					valsStr := ""

					for j := 0; j < len(vv.([]interface{}));j++ {
						valsStr = valsStr + "?"

						if j < len(vv.([]interface{})) -1 {
							valsStr = valsStr + ","
						}
					}
					where  = where + fmt.Sprintf(" AND `%s` NOT IN(%s) ",k,valsStr)
					valList = append(valList,vv.([]interface{})...)

					break}

				case "in"		: {

					valsStr := ""

					for j := 0; j < len(vv.([]interface{}));j++ {
						valsStr = valsStr + "?"

						if j < len(vv.([]interface{})) -1 {
							valsStr = valsStr + ","
						}
					}
					where  = where + fmt.Sprintf(" AND `%s` NOT IN(%s) ",k,valsStr)
					valList = append(valList,vv.([]interface{})...)
					break
				}

				}
			}
		}
	}

	if len(where) > 2 {
		where = string([]byte(where)[6:])
	}

	return where,valList
}
