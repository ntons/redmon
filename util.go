package remon

import (
	"reflect"
	"unsafe"
)

// Convert byte array to string without allocation
func b2s(b []byte) (s string) {
	return *(*string)(unsafe.Pointer(&b))
}

// Convert string to byte array without allocation
func s2b(s string) (b []byte) {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bh.Data, bh.Len, bh.Cap = sh.Data, sh.Len, sh.Len
	return
}
