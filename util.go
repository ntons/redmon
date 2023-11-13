package redmon

import (
	"errors"
	"reflect"
	"unsafe"
)

var (
	ErrAlreadyExists = errors.New("redmon: already exists")
	ErrNotExists     = errors.New("redmon: not exists")
	ErrMailBoxFull   = errors.New("redmon: mail box full")
)

// If you know for sure that the byte slice won't be mutated,
// you won't get bounds (or GC) issues with the above conversions.
func b2s(buf []byte) (str string) {
	return *(*string)(unsafe.Pointer(&buf))
}
func s2b(str string) (buf []byte) {
	*(*string)(unsafe.Pointer(&buf)) = str
	(*reflect.SliceHeader)(unsafe.Pointer(&buf)).Cap = len(str)
	return
}
