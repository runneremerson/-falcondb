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

const (
	LOCK_KEY_NUM  = 1024
	FDB_ITEM_SIZE = unsafe.Sizeof(C.fdb_item_t{})
	INT_SIZE      = unsafe.Sizeof(C.int(0))
)

func ConvertCItemPointer2GoByte(items *C.fdb_item_t, i int, value *FdbValue) {
	var item *C.fdb_item_t
	item = (*C.fdb_item_t)(unsafe.Pointer(uintptr(unsafe.Pointer(items)) + uintptr(i)*FDB_ITEM_SIZE))
	if unsafe.Pointer(item.data_) != CNULL {
		value.val = C.GoBytes(unsafe.Pointer(item.data_), C.int(item.data_len_))
	} else {
		value.val = nil
	}
	value.ret = int(item.retval_)
}

func ConvertCIntPointer2Go(integers *C.int, i int, val *int) {
	if unsafe.Pointer(integers) != CNULL {
		*val = int(*(*C.int)(unsafe.Pointer(uintptr(unsafe.Pointer(integers)) + uintptr(i)*INT_SIZE)))
	}
}

func getLockID(key string) uint32 {
	v := crc32.ChecksumIEEE([]byte(key))
	return uint32(v % uint32(LOCK_KEY_NUM))
}

type FdbLock struct {
	pref *sync.RWMutex
	lock sync.RWMutex
}

func (flock *FdbLock) acquire() {
	if flock.pref != nil {
		flock.pref.RLock()
	}
	flock.lock.Lock()
}

func (flock *FdbLock) release() {
	flock.lock.Unlock()
	if flock.pref != nil {
		flock.pref.RUnlock()
	}
}

type FdbSlot struct {
	context  *C.fdb_context_t
	slot     uint64
	readCnt  uint64
	writeCnt uint64
	lockSlot FdbLock
	lockKeys []FdbLock
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
		fdb.slots[i].lockSlot.pref = nil
		for j := 0; j < LOCK_KEY_NUM; j++ {
			fdb.slots[i].lockKeys[j].pref = &(fdb.slots[i].lockSlot.lock)
		}
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

	var val FdbValue
	if iRet == 0 {
		ConvertCItemPointer2GoByte(pitem_val, 0, &val)
	}

	defer C.destroy_fdb_item_array(pitem_val, C.size_t(1))

	return val.val, &FdbError{retcode: iRet}
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

func (slot *FdbSlot) Del(key []byte) error {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	csKey := (*C.char)(unsafe.Pointer(&key[0]))

	var item_key C.fdb_item_t
	item_key.data_ = csKey
	item_key.data_len_ = C.uint64_t(len(key))

	ret := C.fdb_del(slot.context, C.uint64_t(slot.slot), &item_key)
	iRet := int(ret)

	return &FdbError{retcode: iRet}
}

func (slot *FdbSlot) MSet(kvs []FdbValue, opt int) ([]FdbValue, error) {
	lock := slot.fetchSlotLock()
	lock.acquire()
	defer lock.release()

	item_kvs := make([]C.fdb_item_t, len(kvs))

	for i := 0; i < len(kvs); i++ {
		item_kvs[i].data_ = (*C.char)(unsafe.Pointer(&(kvs[i].val[0])))
		item_kvs[i].data_len_ = C.uint64_t(len(kvs[i].val))
	}

	rets := (*C.int)(CNULL)
	ret := C.fdb_mset(slot.context, C.uint64_t(slot.slot), C.size_t(len(kvs)), (*C.fdb_item_t)(unsafe.Pointer(&item_kvs[0])), &rets, C.int(opt))

	iRet := int(ret)
	var retvalues []FdbValue
	if iRet == 0 {
		length := len(kvs) / 2
		retvalues = make([]FdbValue, length)
		for i := 0; i < length; i++ {
			tmpInt := int(0)
			ConvertCIntPointer2Go(rets, i, &tmpInt)
			retvalues[i].ret = tmpInt
		}
	}
	return retvalues, &FdbError{retcode: iRet}
}

func (slot *FdbSlot) MGet(keys []FdbValue) ([]FdbValue, error) {
	lock := slot.fetchSlotLock()
	lock.acquire()
	defer lock.release()

	item_keys := make([]C.fdb_item_t, len(keys))
	for i := 0; i < len(keys); i++ {
		item_keys[i].data_ = (*C.char)(unsafe.Pointer(&(keys[i].val[0])))
		item_keys[i].data_len_ = C.uint64_t(len(keys[i].val))
	}

	item_vals := (*C.fdb_item_t)(CNULL)

	ret := C.fdb_mget(slot.context, C.uint64_t(slot.slot), C.size_t(len(keys)), (*C.fdb_item_t)(unsafe.Pointer(&item_keys[0])), &item_vals)
	iRet := int(ret)

	var retvalues []FdbValue
	if iRet == 0 {
		retvalues = make([]FdbValue, len(keys))
		for i := 0; i < len(keys); i++ {
			ConvertCItemPointer2GoByte(item_vals, i, &retvalues[i])
		}
	}
	defer C.destroy_fdb_item_array(item_vals, C.size_t(len(keys)))

	return retvalues, &FdbError{retcode: iRet}
}
