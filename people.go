package jsonql

import (
	"reflect"
	"strings"
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
