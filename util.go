package remon

import (
	"reflect"
	"time"
	"unsafe"
)

func b2s(b []byte) (s string) {
	return *(*string)(unsafe.Pointer(&b))
}

func s2b(s string) (b []byte) {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bh.Data, bh.Len, bh.Cap = sh.Data, sh.Len, sh.Len
	return
}

func minIntToDuration(a, b int) time.Duration {
	if a < b {
		return time.Duration(a)
	} else {
		return time.Duration(b)
	}
}
