package utils

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/weixinhost/litedb"
)

func ParseWhereMap(wheres interface{}) (string, []interface{}) {

	whereMap := make(map[string]interface{}, 0)
	valList := make([]interface{}, 0)

	if reflect.ValueOf(wheres).IsValid() {
		if reflect.TypeOf(wheres).Kind() == reflect.Map {
			whereMap = wheres.(map[string]interface{})
		}
	}

	where := "1 "

	for k, v := range whereMap {

		vs := litedb.ToStr(v)
		if reflect.TypeOf(v).Kind() != reflect.Map {
			where = where + fmt.Sprintf(" AND `%s` = ? ", k)
			valList = append(valList, vs)
		}
	}

	for k, v := range whereMap {

		if reflect.TypeOf(v).Kind() == reflect.Map {

			vi := v.(map[string]interface{})

			t, ok1 := vi["type"]

			vv, ok2 := vi["value"]

			if ok1 && ok2 {
				
				tStr := strings.ToLower(t.(string))

				switch tStr {

				case "=":
					{

						where = where + fmt.Sprintf(" AND `%s` = ? ", k)
						valList = append(valList, litedb.ToStr(vv))

						break
					}

				case ">":
					{

						where = where + fmt.Sprintf(" AND `%s` > ? ", k)
						valList = append(valList, litedb.ToStr(vv))

						break
					}
				case "<":
					{

						where = where + fmt.Sprintf(" AND `%s` < ? ", k)
						valList = append(valList, litedb.ToStr(vv))

						break
					}

				case "<=":
					{

						where = where + fmt.Sprintf(" AND `%s` <= ? ", k)
						valList = append(valList, litedb.ToStr(vv))

						break
					}

				case ">=":
					{

						where = where + fmt.Sprintf(" AND `%s` >= ? ", k)
						valList = append(valList, litedb.ToStr(vv))

						break
					}

				case "<>":
					{

						where = where + fmt.Sprintf(" AND `%s` <> ? ", k)
						valList = append(valList, litedb.ToStr(vv))

						break
					}
				case "like":
					{

						where = where + fmt.Sprintf(" AND `%s` LIKE ? ", k)
						valList = append(valList, litedb.ToStr(vv))

						break
					}

				case "not in":
					{

						valsStr := ""

						for j := 0; j < len(vv.([]interface{})); j++ {
							valsStr = valsStr + "?"

							if j < len(vv.([]interface{}))-1 {
								valsStr = valsStr + ","
							}
						}
						where = where + fmt.Sprintf(" AND `%s` NOT IN(%s) ", k, valsStr)
						valList = append(valList, vv.([]interface{})...)

						break
					}

				case "in":
					{

						valsStr := ""

						for j := 0; j < len(vv.([]interface{})); j++ {
							valsStr = valsStr + "?"

							if j < len(vv.([]interface{}))-1 {
								valsStr = valsStr + ","
							}
						}
						where = where + fmt.Sprintf(" AND `%s` IN(%s) ", k, valsStr)
						valList = append(valList, vv.([]interface{})...)
						break
					}

				}
			}
		}
	}

	if len(where) > 2 {
		where = string([]byte(where)[6:])
	}

	return where, valList
}
