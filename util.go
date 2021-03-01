package remon

import (
	"reflect"
	"unsafe"
)

// If you know for sure that the byte slice won't be mutated,
// you won't get bounds (or GC) issues with the above conversions.
func fastBytesToString(buf []byte) (str string) {
	return *(*string)(unsafe.Pointer(&buf))
}
func fastStringToBytes(str string) (buf []byte) {
	*(*string)(unsafe.Pointer(&buf)) = str
	(*reflect.SliceHeader)(unsafe.Pointer(&buf)).Cap = len(str)
	return
}

// int64 slice for sorting
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
