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
		value.Val = C.GoBytes(unsafe.Pointer(item.data_), C.int(item.data_len_))
		ret := int(item.retval_)
		value.Ret = ret
	} else {
		value.Val = nil
		value.Ret = 0
	}
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

func (slot *FdbSlot) Drop() {
	C.fdb_drop_slot(slot.context, C.uint64_t(slot.slot))
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

	defer C.destroy_fdb_item_array(pitem_val, C.size_t(1))

	if iRet == 0 {
		var val FdbValue
		ConvertCItemPointer2GoByte(pitem_val, 0, &val)
		return val.Val, nil
	}

	return nil, &FdbError{retcode: iRet}
}

func (slot *FdbSlot) Set(key []byte, val []byte) error {
	_, err := slot.set(key, val, 0, 1)
	return err
}

func (slot *FdbSlot) StrLen(key []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) SetNX(key, val []byte) (int64, error) {
	return slot.set(key, val, 0, 0)
}

func (slot *FdbSlot) SetEX(key []byte, duration int64, val []byte) error {
	_, err := slot.set(key, val, duration, 1)
	return err
}

func (slot *FdbSlot) set(key []byte, val []byte, duration int64, opt int) (int64, error) {
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

	ef := C.int(1)

	ret := C.fdb_set(slot.context, C.uint64_t(slot.slot), &item_key, &item_val, C.int64_t(duration), C.int(opt), &ef)

	if int(ret) == 0 {
		return int64(ef), nil
	}
	return int64(ef), &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) IncrBy(key []byte, increment int64) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) IncrByFloat(key []byte, increment float64) (float64, error) {
	return 0.0, nil
}

func (slot *FdbSlot) Append(key []byte, value []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) GetSet(key []byte, value []byte) ([]byte, error) {
	return nil, nil
}

func (slot *FdbSlot) Del(keys ...[]byte) (int64, error) {
	lock := slot.fetchSlotLock()
	lock.acquire()
	defer lock.release()

	var cnt int64 = 0
	for i := 0; i < len(keys); i++ {

		csKey := (*C.char)(unsafe.Pointer(&keys[i][0]))

		var item_key C.fdb_item_t
		item_key.data_ = csKey
		item_key.data_len_ = C.uint64_t(len(keys[i]))

		ret := C.fdb_del(slot.context, C.uint64_t(slot.slot), &item_key)
		if int(ret) == 0 {
			cnt++
		}
	}

	return cnt, nil
}

func (slot *FdbSlot) MSet(kvs []FdbPair) ([]error, error) {
	return slot.mset(kvs, 1)
}

func (slot *FdbSlot) mset(kvs []FdbPair, opt int) ([]error, error) {
	lock := slot.fetchSlotLock()
	lock.acquire()
	defer lock.release()

	item_kvs := make([]C.fdb_item_t, 2*len(kvs))

	j := 0
	for i := 0; i < len(kvs); i++ {
		item_kvs[j].data_ = (*C.char)(unsafe.Pointer(&(kvs[i].Key[0])))
		item_kvs[j].data_len_ = C.uint64_t(len(kvs[i].Key))
		j++

		item_kvs[j].data_ = (*C.char)(unsafe.Pointer(&(kvs[i].Value[0])))
		item_kvs[j].data_len_ = C.uint64_t(len(kvs[i].Value))
		j++
	}

	rets := (*C.int)(CNULL)
	ret := C.fdb_mset(slot.context, C.uint64_t(slot.slot), C.size_t(2*len(kvs)), (*C.fdb_item_t)(unsafe.Pointer(&item_kvs[0])), &rets, C.int(opt))

	var retvalues []error
	if int(ret) == 0 {
		length := len(kvs)
		retvalues = make([]error, length)
		for i := 0; i < length; i++ {
			tmpInt := int(0)
			ConvertCIntPointer2Go(rets, i, &tmpInt)
			if tmpInt == 0 {
				retvalues[i] = nil
			} else {
				retvalues[i] = &FdbError{retcode: tmpInt}
			}
		}
		return retvalues, nil
	}
	return retvalues, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) MGet(keys ...[]byte) ([][]byte, error) {
	lock := slot.fetchSlotLock()
	lock.acquire()
	defer lock.release()

	item_keys := make([]C.fdb_item_t, len(keys))
	for i := 0; i < len(keys); i++ {
		item_keys[i].data_ = (*C.char)(unsafe.Pointer(&(keys[i][0])))
		item_keys[i].data_len_ = C.uint64_t(len(keys[i]))
	}

	item_vals := (*C.fdb_item_t)(CNULL)

	ret := C.fdb_mget(slot.context, C.uint64_t(slot.slot), C.size_t(len(keys)), (*C.fdb_item_t)(unsafe.Pointer(&item_keys[0])), &item_vals)
	defer C.destroy_fdb_item_array(item_vals, C.size_t(len(keys)))

	if int(ret) == 0 {
		var value FdbValue
		retvalues := make([][]byte, len(keys))
		for i := 0; i < len(keys); i++ {
			ConvertCItemPointer2GoByte(item_vals, i, &value)
			retvalues[i] = value.Val
			if !(value.Ret == 0 || value.Ret == FDB_OK_NOT_EXIST) {
				return nil, &FdbError{retcode: value.Ret}
			}
		}
		return retvalues, nil
	}

	return nil, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) Exists(key []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) HGet(key, field []byte) ([]byte, error) {
	return nil, nil
}

func (slot *FdbSlot) HSet(key, field, value []byte) (int64, error) {
	return 0, nil
}
func (slot *FdbSlot) HMget(key []byte, fields ...[]byte) ([][]byte, error) {
	return nil, nil
}
func (slot *FdbSlot) HDel(key []byte, args ...[]byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) HLen(key []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) HIncrByFloat(key []byte, field []byte, delta float64) (float64, error) {
	return 0.0, nil
}

func (slot *FdbSlot) HIncrBy(key []byte, field []byte, delta int64) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) HExists(key, field []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) HSetNX(key, field, value []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) HGetAll(key []byte) ([]FdbFVPair, error) {
	return nil, nil
}
func (slot *FdbSlot) HMset(key []byte, args ...FdbFVPair) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) ZAdd(key []byte, args ...FdbScore) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) ZCard(key []byte) (int64, error) {
	return 0, nil
}
func (slot *FdbSlot) ZScore(key []byte, member []byte) (int64, error) {
	return 0, nil
}
func (slot *FdbSlot) ZRem(key []byte, members ...[]byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) ZCount(key []byte, min int64, max int64) (int64, error) {
	return 0, nil
}
func (slot *FdbSlot) ZRemRangeByRank(key []byte, start int, stop int) (int64, error) {
	return 0, nil
}
func (slot *FdbSlot) ZRemRangeByScore(key []byte, min int64, max int64) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) ZRemRangeByLex(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) ZLexCount(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	return 0, nil
}
func (slot *FdbSlot) ZRange(key []byte, start int, stop int) ([]FdbScore, error) {
	return nil, nil
}

func (slot *FdbSlot) ZRank(key []byte, member []byte) (int64, error) {
	return 0, nil
}
func (slot *FdbSlot) ZRevRank(key []byte, member []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) ZIncrBy(key []byte, delta int64, member []byte) (int64, error) {
	return 0, nil
}

//set
func (slot *FdbSlot) SMembers(key []byte) ([][]byte, error) {
	return nil, nil
}

func (slot *FdbSlot) SIsMember(key []byte, member []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) SCard(key []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) SRem(key []byte, args ...[]byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) SAdd(key []byte, args ...[]byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) PExpireAt(key []byte, when int64) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) PTTL(key []byte) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) Type(key []byte) (string, error) {
	return "", nil
}

func (slot *FdbSlot) Persist(key []byte) (int64, error) {
	return 0, nil
}
