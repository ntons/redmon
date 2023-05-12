package remon

import (
	"errors"
	"reflect"
	"unsafe"
)

var (
	ErrAlreadyExists = errors.New("remon: already exists")
	ErrNotExists     = errors.New("remon: not exists")
	ErrMailBoxFull   = errors.New("remon: mail box full")
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
