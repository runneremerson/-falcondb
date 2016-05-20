package fdb

import (
	"unsafe"
)

var CNULL = unsafe.Pointer(uintptr(0))

type FdbValue struct {
	Val []byte
	Ret error
}

type FdbPair struct {
	Key []byte
	Val []byte
	Ret error
}

type FdbScore struct {
	Field []byte
	Score []byte
}

type FdbError struct {
	retcode int
	message string
}

func (err *FdbError) Code() int {
	return err.retcode
}

func (err *FdbError) Error() string {
	return err.message
}
