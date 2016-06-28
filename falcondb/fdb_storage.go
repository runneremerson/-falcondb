package fdb

import (
	"fmt"
	"unsafe"
)

const (
	FDB_OK_HYPERLOGLOG_NOT_EXIST    = 8
	FDB_OK_HYPERLOGLOG_EXIST        = 7
	FDB_OK_SUB_NOT_EXIST            = 6
	FDB_OK_BUT_ALREADY_EXIST        = 5
	FDB_ERR_EXPIRE_TIME_OUT         = 4
	FDB_OK_NOT_EXIST                = 3
	FDB_OK_BUT_CONE                 = 2
	FDB_OK_BUT_CZERO                = 1
	FDB_OK                          = 0
	FDB_ERR                         = -1
	FDB_ERR_LENGTHXERO              = -2
	FDB_ERR_REACH_MAXMEMORY         = -3
	FDB_ERR_UNKNOWN_COMMAND         = -4
	FDB_ERR_WRONG_NUMBER_ARGUMENTS  = -5
	FDB_ERR_OPERATION_NOT_PERMITTED = -6
	FDB_ERR_QUEUED                  = -7
	FDB_ERR_LOADINGERR              = -8
	FDB_ERR_FORBIDDEN_ABOUT_PUBSUB  = -9
	FDB_ERR_FORBIDDEN_INFO_SLAVEOF  = -10
	FDB_ERR_VERSION_ERROR           = -11
	FDB_OK_RANGE_HAVE_NONE          = -12
	FDB_ERR_WRONG_TYPE_ERROR        = -13
	FDB_ERR_CNEGO_ERROR             = -14
	FDB_ERR_IS_NOT_NUMBER           = -15
	FDB_ERR_INCDECR_OVERFLOW        = -16
	FDB_ERR_IS_NOT_INTEGER          = -17
	FDB_ERR_MEMORY_ALLOCATE_ERROR   = -18
	FDB_ERR_OUT_OF_RANGE            = -19
	FDB_ERR_IS_NOT_DOUBLE           = -20
	FDB_ERR_SYNTAX_ERROR            = -21
	FDB_ERR_NAMESPACE_ERROR         = -22
	FDB_ERR_DATA_LEN_LIMITED        = -23
	FDB_SAME_OBJECT_ERR             = -24
	FDB_ERR_BUCKETID                = -25
	FDB_ERR_NOT_SET_EXPIRE          = -26
	FDB_NOT_SUPPORT_METHOD          = -27
)

var CNULL = unsafe.Pointer(uintptr(0))

type FdbValue struct {
	Val []byte
	Ret int
}

type FdbFVPair struct {
	Field []byte
	Value []byte
}

type FdbPair struct {
	Key   []byte
	Value []byte
}

type FdbScore struct {
	Score  float64
	Member []byte
}

type FdbError struct {
	retcode int
	message string
}

func (err *FdbError) Code() int {
	return err.retcode
}

func (err *FdbError) Error() string {
	if len(err.message) == 0 {
		return fmt.Sprintf("FdbError[%d]", err.retcode)
	}
	return err.message
}
