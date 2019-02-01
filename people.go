package jsonql

import (
	"reflect"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// People ...
type People struct {
	Firstname   string `json:"firstname,omitempty"`
	Lastname    string `json:"lastname,omitempty"`
	Email       string `json:"email,omitempty"`
	Phonenumber string `json:"phonenumber,omitempty"`
}

// StructAttrName ...
func (p *People) StructAttrName() []string {
	len := reflect.TypeOf(p).Elem().NumField()
	var names []string

	for i := 0; i < len; i++ {
		name := reflect.TypeOf(p).Elem().Field(i).Name
		names = append(names, strings.ToLower(name))
	}

	return names
}

// GetFieldsLen ...
func (p *People) GetFieldsLen() int {
	return reflect.TypeOf(p).Elem().NumField()
}

// GetFields ...
func (p *People) GetFields(i int) string {
	return reflect.TypeOf(p).Elem().Field(i).Name
}

func checkRow(schema sql.Schema, row sql.Row) error {
	if len(row) != len(schema) {
		return sql.ErrUnexpectedRowLength.New(len(schema), len(row))
	}

	for i, value := range row {
		c := schema[i]
		if !c.Check(value) {
			return sql.ErrInvalidType.New(value)
		}
	}

	return nil
}
