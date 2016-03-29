package utils

import (
	"testing"
)

func TestBasic(t *testing.T) {

	where := map[string]interface{}{
		"field_1"	: 1,
		"field_2"	: "abcd",
		"field_3"	: 1.456,
	}
	t.Error(ParseWhereMap(where))
}

func TestEmpty(t *testing.T) {

	where := map[string]interface{} {

	}

	t.Error(ParseWhereMap(where))
}

func TestAdvance(t *testing.T) {

	where := map[string]interface{} {
		"field_1"	: map[string]interface{} {
			"type"	: ">",
			"value"	: 123,
		},
		"field_2"	: map[string]interface{} {
			"type"	: "<",
			"value"	: 123,
		},
		"field_3"	: map[string]interface{} {
			"type"	: "not in",
			"value"	: []interface{}{1,2,3,4},
		},
	}

	t.Error(ParseWhereMap(where))

}



func TestBasicAdvance(t *testing.T) {

	where := map[string]interface{} {
		"a_1"		: 1,
		"a_2"		: "sssss",
		"a_3"		: 12312.12312,
		"field_1"	: map[string]interface{} {
			"type"	: ">",
			"value"	: 123,
		},
		"field_2"	: map[string]interface{} {
			"type"	: "<",
			"value"	: 123,
		},
		"field_3"	: map[string]interface{} {
			"type"	: "not in",
			"value"	: []interface{}{1,2,3,4},
		},
	}

	t.Error(ParseWhereMap(where))
}



func TestError(t *testing.T) {

	where := false
	t.Error(ParseWhereMap(where))
}






