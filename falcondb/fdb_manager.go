package fdb

/*
#cgo  linux CFLAGS: -std=gnu99 -W -I./ -I../deps/rocksdb-4.2/include  -DUSE_TCMALLOC=1 -DUSE_INT=1
#cgo  LDFLAGS:	 -L/usr/local/lib  -L../deps/rocksdb-4.2 -L../deps/snappy-1.1.1/.libs -lpthread -lm -lsnappy -ltcmalloc -lrocksdb
#include "fdb_session.h"
#include "fdb_context.h"
*/
import "C"

import (
	"hash/crc32"
	"sync"
	"unsafe"
)

var CNULL = unsafe.Pointer(uintptr(0))

const (
	LOCK_KEY_NUM  = 1024
	FDB_ITEM_SIZE = unsafe.Sizeof(C.fdb_item_t{})
)

func ConvertCItemPointer2GoByte(items *C.fdb_item_t, i int) []byte {
	var item *C.fdb_item_t
	var result []byte
	item = (*C.fdb_item_t)(unsafe.Pointer(uintptr(unsafe.Pointer(items)) + uintptr(i)*FDB_ITEM_SIZE))
	if unsafe.Pointer(item.data_) != CNULL {
		result = C.GoBytes(unsafe.Pointer(item.data_), C.int(item.data_len_))
	} else {
		result = nil
	}
	return result
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

func getLockID(key string) uint32 {
	v := crc32.ChecksumIEEE([]byte(key))
	return uint32(v % uint32(LOCK_KEY_NUM))
}

type FdbLock struct {
	lock sync.RWMutex
}

func (flock *FdbLock) acquire() {
	flock.lock.Lock()
}

func (flock *FdbLock) release() {
	flock.lock.Unlock()
}

type FdbSlot struct {
	context  *C.fdb_context_t
	slot     uint64
	readCnt  uint64
	writeCnt uint64
	lockKeys []FdbLock
	lockSlot FdbLock
}

type FdbManager struct {
	inited bool
	lock   FdbLock
	slots  []*FdbSlot
}

func (fdb *FdbManager) GetFdbSlotNumber() int {
	return len(fdb.slots)
}

func (fdb *FdbManager) GetFdbSlot(id uint64) *FdbSlot {
	return fdb.slots[id]
}

func NewFdbManager() (*FdbManager, error) {
	fdb := &FdbManager{
		inited: false,
	}
	return fdb, nil
}

func (fdb *FdbManager) InitDB(file_path string, cache_size int, write_buffer_size int, num_slots int) error {
	fdb.lock.acquire()
	defer fdb.lock.release()

	if fdb.inited {
		return nil
	}
	csPath := C.CString(file_path)
	context := C.fdb_context_create(csPath, C.size_t(cache_size), C.size_t(write_buffer_size), C.size_t(num_slots))

	fdb.slots = make([]*FdbSlot, num_slots)
	for i := 0; i < num_slots; i++ {
		fdb.slots[i] = &FdbSlot{
			context:  context,
			slot:     uint64(i + 1),
			readCnt:  uint64(0),
			writeCnt: uint64(0),
		}
		fdb.slots[i].lockKeys = make([]FdbLock, LOCK_KEY_NUM)
	}
	fdb.inited = true
	return nil
}

func (slot *FdbSlot) fetchKeysLock(key string) *FdbLock {
	id := getLockID(key)
	return &(slot.lockKeys[id])
}

func (slot *FdbSlot) fetchSlotLock() *FdbLock {
	return &(slot.lockSlot)
}

func (slot *FdbSlot) GetSlotID() uint64 {
	return slot.slot
}

func (slot *FdbSlot) Get(key []byte) ([]byte, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	csKey := (*C.char)(unsafe.Pointer(&key[0]))

	var item_key C.fdb_item_t
	item_key.data_ = csKey
	item_key.data_len_ = C.uint64_t(len(key))

	var pitem_val *C.fdb_item_t
	pitem_val = (*C.fdb_item_t)(CNULL)

	ret := C.fdb_get(slot.context, C.uint64_t(slot.slot), &item_key, &pitem_val)
	iRet := int(ret)

	var val []byte = nil
	if iRet == 0 {
		val = ConvertCItemPointer2GoByte(pitem_val, 0)
	}

	defer C.destroy_fdb_item_array(pitem_val, C.size_t(1))

	return val, &FdbError{retcode: iRet}
}

func (slot *FdbSlot) Set(key []byte, val []byte, exptime uint32, opt int) error {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	csKey := (*C.char)(unsafe.Pointer(&key[0]))
	csVal := (*C.char)(unsafe.Pointer(&val[0]))

	var item_key C.fdb_item_t
	item_key.data_ = csKey
	item_key.data_len_ = C.uint64_t(len(key))

	var item_val C.fdb_item_t
	item_val.data_ = csVal
	item_val.data_len_ = C.uint64_t(len(val))

	ret := C.fdb_set(slot.context, C.uint64_t(slot.slot), &item_key, &item_val, C.uint64_t(exptime), C.int(opt))
	iRet := int(ret)

	return &FdbError{retcode: iRet}
}
