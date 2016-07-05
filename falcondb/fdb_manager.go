package fdb

/*
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
	DOUBLE_SIZE   = unsafe.Sizeof(C.double(0.0))
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

func ConvertCDoublePointer2Go(floats *C.double, i int, value *float64) {
	if unsafe.Pointer(floats) != CNULL {
		*value = float64(*(*C.double)(unsafe.Pointer(uintptr(unsafe.Pointer(floats)) + uintptr(i)*DOUBLE_SIZE)))
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
	fdb      *FdbManager
	slot     uint64
	readCnt  uint64
	writeCnt uint64
	lockSlot FdbLock
	lockKeys []FdbLock
}

func (slot *FdbSlot) Drop() {
	lock := slot.fetchSlotLock()
	lock.acquire()
	defer lock.release()

	C.fdb_drop_slot(slot.fdb.ctx, C.uint64_t(slot.slot))
}

type FdbManager struct {
	inited bool
	ctx    *C.fdb_context_t
	lock   FdbLock
	slots  []*FdbSlot
}

func (fdb *FdbManager) GetFdbSlotNumber() int {
	return len(fdb.slots)
}

func (fdb *FdbManager) GetFdbSlot(id uint64) *FdbSlot {
	return fdb.slots[id]
}

func NewFdbManager() *FdbManager {
	fdb := &FdbManager{
		inited: false,
	}
	return fdb
}

func (fdb *FdbManager) DropDB(file_path string) {
	fdb.lock.acquire()
	defer fdb.lock.release()

	csPath := C.CString(file_path)
	defer C.free(unsafe.Pointer(csPath))

	C.fdb_drop_db(csPath)
	fdb.inited = false
}

func (fdb *FdbManager) InitDB(file_path string, cache_size int, write_buffer_size int, num_slots int) error {
	fdb.lock.acquire()
	defer fdb.lock.release()

	if fdb.inited {
		return nil
	}
	csPath := C.CString(file_path)
	defer C.free(unsafe.Pointer(csPath))

	fdb.ctx = C.fdb_context_create(csPath, C.size_t(cache_size), C.size_t(write_buffer_size), C.size_t(num_slots))

	fdb.slots = make([]*FdbSlot, num_slots)
	for i := 0; i < num_slots; i++ {
		fdb.slots[i] = &FdbSlot{
			fdb:      fdb,
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

func (fdb *FdbManager) CloseDB() {
	fdb.lock.acquire()
	defer fdb.lock.release()

	if fdb.inited {
		C.fdb_context_destroy(fdb.ctx)
		fdb.inited = false
	}
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

	var item_val *C.fdb_item_t
	item_val = (*C.fdb_item_t)(CNULL)

	ret := C.fdb_get(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_val)
	iRet := int(ret)

	if iRet == 0 {
		defer C.destroy_fdb_item_array(item_val, C.size_t(1))

		var val FdbValue
		ConvertCItemPointer2GoByte(item_val, 0, &val)
		return val.Val, nil
	} else if iRet > 0 {
		return nil, nil
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

	ret := C.fdb_set(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_val, C.int64_t(duration), C.int(opt), &ef)

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

		count := C.int64_t(0)

		ret := C.fdb_del(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &count)
		if int(ret) == 0 {
			cnt += int64(count)
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

	prim_rets := (*C.int)(CNULL)
	ret := C.fdb_mset(slot.fdb.ctx, C.uint64_t(slot.slot), C.size_t(2*len(kvs)), (*C.fdb_item_t)(unsafe.Pointer(&item_kvs[0])), &prim_rets, C.int(opt))

	var retvalues []error
	if int(ret) == 0 {
		defer C.free_int_array(prim_rets)

		length := len(kvs)
		retvalues = make([]error, length)
		for i := 0; i < length; i++ {
			tmpInt := int(0)
			ConvertCIntPointer2Go(prim_rets, i, &tmpInt)
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

	ret := C.fdb_mget(slot.fdb.ctx, C.uint64_t(slot.slot), C.size_t(len(keys)), (*C.fdb_item_t)(unsafe.Pointer(&item_keys[0])), &item_vals)

	if int(ret) == 0 {
		defer C.destroy_fdb_item_array(item_vals, C.size_t(len(keys)))

		retvalues := make([][]byte, len(keys))
		var value FdbValue
		for i := 0; i < len(keys); i++ {
			ConvertCItemPointer2GoByte(item_vals, i, &value)
			retvalues[i] = value.Val
			if !(value.Ret >= 0) {
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
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_fld C.fdb_item_t
	item_fld.data_ = (*C.char)(unsafe.Pointer(&field[0]))
	item_fld.data_len_ = C.uint64_t(len(field))

	var item_val *C.fdb_item_t
	item_val = (*C.fdb_item_t)(CNULL)

	ret := C.fdb_hget(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_fld, &item_val)

	if int(ret) == 0 {
		defer C.destroy_fdb_item_array(item_val, C.size_t(1))

		var val FdbValue
		ConvertCItemPointer2GoByte(item_val, 0, &val)
		return val.Val, nil
	} else if int(ret) > 0 {
		return nil, nil
	}

	return nil, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) HSet(key, field, value []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_fld C.fdb_item_t
	item_fld.data_ = (*C.char)(unsafe.Pointer(&field[0]))
	item_fld.data_len_ = C.uint64_t(len(field))

	var item_val C.fdb_item_t
	item_val.data_ = (*C.char)(unsafe.Pointer(&value[0]))
	item_val.data_len_ = C.uint64_t(len(value))

	cnt := C.int64_t(0)
	ret := C.fdb_hset(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_fld, &item_val, &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}
func (slot *FdbSlot) HMget(key []byte, fields ...[]byte) ([][]byte, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_flds := make([]C.fdb_item_t, len(fields))
	for i := 0; i < len(fields); i++ {
		item_flds[i].data_ = (*C.char)(unsafe.Pointer(&(fields[i][0])))
		item_flds[i].data_len_ = C.uint64_t(len(fields[i]))
	}
	item_vals := (*C.fdb_item_t)(CNULL)

	ret := C.fdb_hmget(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.size_t(len(fields)), (*C.fdb_item_t)(unsafe.Pointer(&item_flds[0])), &item_vals)

	if int(ret) == 0 {
		defer C.destroy_fdb_item_array(item_vals, C.size_t(len(fields)))

		retvalues := make([][]byte, len(fields))
		var value FdbValue
		for i := 0; i < len(fields); i++ {
			ConvertCItemPointer2GoByte(item_vals, i, &value)
			if value.Ret == 0 {
				retvalues[i] = value.Val
			} else if value.Ret > 0 {
				retvalues[i] = nil
			} else {
				return nil, &FdbError{retcode: value.Ret}
			}
		}
		return retvalues, nil
	} else if int(ret) > 0 {
		retvalues := make([][]byte, len(fields))
		for i := 0; i < len(fields); i++ {
			retvalues[i] = nil
		}
		return retvalues, nil
	}

	return nil, &FdbError{retcode: int(ret)}
}
func (slot *FdbSlot) HDel(key []byte, fields ...[]byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_flds := make([]C.fdb_item_t, len(fields))
	for i := 0; i < len(fields); i++ {
		item_flds[i].data_ = (*C.char)(unsafe.Pointer(&(fields[i][0])))
		item_flds[i].data_len_ = C.uint64_t(len(fields[i]))
	}

	cnt := C.int64_t(0)
	ret := C.fdb_hdel(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.size_t(len(fields)), (*C.fdb_item_t)(unsafe.Pointer(&item_flds[0])), &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}

	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) HLen(key []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	length := C.int64_t(0)
	ret := C.fdb_hlen(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &length)

	if int(ret) == 0 {
		return int64(length), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) HIncrByFloat(key []byte, field []byte, delta float64) (float64, error) {
	return 0.0, nil
}

func (slot *FdbSlot) HIncrBy(key []byte, field []byte, delta int64) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_fld C.fdb_item_t
	item_fld.data_ = (*C.char)(unsafe.Pointer(&field[0]))
	item_fld.data_len_ = C.uint64_t(len(field))

	result := C.int64_t(0)
	ret := C.fdb_hincrby(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_fld, C.int64_t(delta), &result)
	if int(ret) == 0 {
		return int64(result), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) HExists(key, field []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_fld C.fdb_item_t
	item_fld.data_ = (*C.char)(unsafe.Pointer(&field[0]))
	item_fld.data_len_ = C.uint64_t(len(field))

	cnt := C.int64_t(0)
	ret := C.fdb_hexists(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_fld, &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) HSetNX(key, field, value []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_fld C.fdb_item_t
	item_fld.data_ = (*C.char)(unsafe.Pointer(&field[0]))
	item_fld.data_len_ = C.uint64_t(len(field))

	var item_val C.fdb_item_t
	item_val.data_ = (*C.char)(unsafe.Pointer(&value[0]))
	item_val.data_len_ = C.uint64_t(len(value))

	cnt := C.int64_t(0)
	ret := C.fdb_hsetnx(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_fld, &item_val, &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) HGetAll(key []byte) ([][]byte, [][]byte, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_fvs := (*C.fdb_item_t)(CNULL)
	length := C.int64_t(0)
	ret := C.fdb_hgetall(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_fvs, &length)

	if int(ret) == 0 {
		defer C.destroy_fdb_item_array(item_fvs, C.size_t(length))

		_length := int(int(length) / 2)
		ind := 0
		retfields := make([][]byte, _length)
		retvalues := make([][]byte, _length)
		for i := 0; i < _length; i++ {
			var field FdbValue
			ConvertCItemPointer2GoByte(item_fvs, ind, &field)
			retfields[i] = field.Val
			ind++

			var value FdbValue
			ConvertCItemPointer2GoByte(item_fvs, ind, &value)
			retvalues[i] = value.Val
			ind++
		}
		return retfields, retvalues, nil
	} else if ret > 0 {
		return nil, nil, nil
	}

	return nil, nil, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) HMset(key []byte, fields, values [][]byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_fvs := make([]C.fdb_item_t, len(fields)*2)
	ind := 0
	for i := 0; i < len(fields); i++ {
		item_fvs[ind].data_ = (*C.char)(unsafe.Pointer(&(fields[i][0])))
		item_fvs[ind].data_len_ = C.uint64_t(len(fields[i]))
		ind++

		item_fvs[ind].data_ = (*C.char)(unsafe.Pointer(&(values[i][0])))
		item_fvs[ind].data_len_ = C.uint64_t(len(values[i]))
		ind++
	}

	cnt := C.int64_t(0)
	ret := C.fdb_hmset(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.size_t(ind), (*C.fdb_item_t)(unsafe.Pointer(&item_fvs[0])), &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) ZAdd(key []byte, members [][]byte, scores []float64) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_mbrs := make([]C.fdb_item_t, len(members))
	prim_scrs := make([]C.double, len(members))
	for i := 0; i < len(members); i++ {
		item_mbrs[i].data_ = (*C.char)(unsafe.Pointer(&(members[i][0])))
		item_mbrs[i].data_len_ = C.uint64_t(len(members[i]))

		prim_scrs[i] = C.double(scores[i])
	}

	cnt := C.int64_t(0)
	ret := C.fdb_zadd(slot.fdb.ctx,
		C.uint64_t(slot.slot),
		&item_key,
		C.size_t(len(members)),
		(*C.double)(unsafe.Pointer(&prim_scrs[0])),
		(*C.fdb_item_t)(unsafe.Pointer(&item_mbrs[0])),
		&cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) ZCard(key []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	size := C.int64_t(0)
	ret := C.fdb_zcard(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &size)
	if int(ret) == 0 {
		return int64(size), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}
func (slot *FdbSlot) ZScore(key []byte, member []byte) (float64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_mbr C.fdb_item_t
	item_mbr.data_ = (*C.char)(unsafe.Pointer(&member[0]))
	item_mbr.data_len_ = C.uint64_t(len(member))

	score := C.double(0.0)
	ret := C.fdb_zscore(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_mbr, &score)
	if int(ret) == 0 {
		return float64(score), nil
	} else if int(ret) > 0 {
		return 0.0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}
func (slot *FdbSlot) ZRem(key []byte, members ...[]byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_mbrs := make([]C.fdb_item_t, len(members))
	for i := 0; i < len(members); i++ {
		item_mbrs[i].data_ = (*C.char)(unsafe.Pointer(&(members[i][0])))
		item_mbrs[i].data_len_ = C.uint64_t(len(members[i]))
	}

	cnt := C.int64_t(0)
	ret := C.fdb_zrem(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.size_t(len(members)), (*C.fdb_item_t)(unsafe.Pointer(&item_mbrs[0])), &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) ZCount(key []byte, min float64, max float64, rangeType uint8) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	cnt := C.int64_t(0)
	ret := C.fdb_zcount(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.double(min), C.double(max), C.uint8_t(rangeType), &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}
func (slot *FdbSlot) ZRemRangeByRank(key []byte, start int, stop int) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	cnt := C.int64_t(0)
	ret := C.fdb_zrem_range_by_rank(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.int(start), C.int(stop), &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}
func (slot *FdbSlot) ZRemRangeByScore(key []byte, min float64, max float64, rangeType uint8) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	cnt := C.int64_t(0)
	ret := C.fdb_zrem_range_by_score(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.double(min), C.double(max), C.uint8_t(rangeType), &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) ZRemRangeByLex(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) ZLexCount(key []byte, min []byte, max []byte, rangeType uint8) (int64, error) {
	return 0, nil
}

func (slot *FdbSlot) zrangeByRank(key []byte, start int, stop int, reverse int, withscore bool) ([][]byte, []float64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_mbrs := (*C.fdb_item_t)(CNULL)
	prim_scrs := (*C.double)(CNULL)
	length := C.int64_t(0)

	ret := C.fdb_zrange(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.int(start), C.int(stop), C.int(reverse), &prim_scrs, &item_mbrs, &length)

	if int(ret) == 0 {
		defer C.free_double_array(prim_scrs)
		defer C.destroy_fdb_item_array(item_mbrs, C.size_t(length))

		_length := int(length)
		retmembers := make([][]byte, _length)
		retscores := make([]float64, _length)
		var value FdbValue
		var score float64
		for i := 0; i < _length; i++ {
			ConvertCItemPointer2GoByte(item_mbrs, i, &value)
			retmembers[i] = value.Val

			ConvertCDoublePointer2Go(prim_scrs, i, &score)
			retscores[i] = score
		}
		if withscore {
			return retmembers, retscores, nil
		} else {
			return retmembers, nil, nil
		}
	} else if ret > 0 {
		return nil, nil, nil
	}
	return nil, nil, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) ZRange(key []byte, start int, stop int, withscore bool) ([][]byte, []float64, error) {
	return slot.zrangeByRank(key, start, stop, 0, withscore)
}

func (slot *FdbSlot) ZRevRange(key []byte, start int, stop int, withscore bool) ([][]byte, []float64, error) {
	return slot.zrangeByRank(key, start, stop, 1, withscore)
}

func (slot *FdbSlot) ZRank(key []byte, member []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_mbr C.fdb_item_t
	item_mbr.data_ = (*C.char)(unsafe.Pointer(&member[0]))
	item_mbr.data_len_ = C.uint64_t(len(member))

	rank := C.int64_t(0)
	ret := C.fdb_zrank(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_mbr, C.int(0), &rank)

	if int(ret) == 0 {
		return int64(rank), nil
	} else if int(ret) > 0 {
		return int64(-1), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}
func (slot *FdbSlot) ZRevRank(key []byte, member []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_mbr C.fdb_item_t
	item_mbr.data_ = (*C.char)(unsafe.Pointer(&member[0]))
	item_mbr.data_len_ = C.uint64_t(len(member))

	rank := C.int64_t(0)
	ret := C.fdb_zrank(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_mbr, C.int(1), &rank)

	if int(ret) == 0 {
		return int64(rank), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) ZIncrBy(key []byte, delta float64, member []byte) (float64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_mbr C.fdb_item_t
	item_mbr.data_ = (*C.char)(unsafe.Pointer(&member[0]))
	item_mbr.data_len_ = C.uint64_t(len(member))

	result := C.double(0.0)
	ret := C.fdb_zincrby(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_mbr, C.double(delta), &result)

	if int(ret) == 0 {
		return float64(result), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

//set
func (slot *FdbSlot) SMembers(key []byte) ([][]byte, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_mbrs := (*C.fdb_item_t)(CNULL)
	length := C.int64_t(0)
	ret := C.fdb_smembers(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_mbrs, &length)

	if int(ret) == 0 {
		defer C.destroy_fdb_item_array(item_mbrs, C.size_t(length))

		_length := int(length)
		retmembers := make([][]byte, _length)
		var value FdbValue
		for i := 0; i < _length; i++ {
			ConvertCItemPointer2GoByte(item_mbrs, i, &value)
			retmembers[i] = value.Val
		}
		return retmembers, nil
	} else if ret > 0 {
		return nil, nil
	}
	return nil, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) SIsMember(key []byte, member []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	var item_mbr C.fdb_item_t
	item_mbr.data_ = (*C.char)(unsafe.Pointer(&member[0]))
	item_mbr.data_len_ = C.uint64_t(len(member))

	cnt := C.int64_t(0)
	ret := C.fdb_sismember(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &item_mbr, &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}

}

func (slot *FdbSlot) SCard(key []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	cnt := C.int64_t(0)
	ret := C.fdb_scard(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}

	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) SRem(key []byte, members ...[]byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_mbrs := make([]C.fdb_item_t, len(members))
	for i := 0; i < len(members); i++ {
		item_mbrs[i].data_ = (*C.char)(unsafe.Pointer(&(members[i][0])))
		item_mbrs[i].data_len_ = C.uint64_t(len(members[i]))
	}

	cnt := C.int64_t(0)
	ret := C.fdb_srem(slot.fdb.ctx, C.uint64_t(slot.slot), &item_key, C.size_t(len(members)), (*C.fdb_item_t)(unsafe.Pointer(&item_mbrs[0])), &cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	} else if int(ret) > 0 {
		return 0, nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) SAdd(key []byte, members ...[]byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	item_mbrs := make([]C.fdb_item_t, len(members))
	for i := 0; i < len(members); i++ {
		item_mbrs[i].data_ = (*C.char)(unsafe.Pointer(&(members[i][0])))
		item_mbrs[i].data_len_ = C.uint64_t(len(members[i]))

	}

	cnt := C.int64_t(0)
	ret := C.fdb_sadd(slot.fdb.ctx,
		C.uint64_t(slot.slot),
		&item_key,
		C.size_t(len(members)),
		(*C.fdb_item_t)(unsafe.Pointer(&item_mbrs[0])),
		&cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) PExpireAt(key []byte, when int64) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	cnt := C.int64_t(0)
	ret := C.fdb_pexpire_at(slot.fdb.ctx,
		C.uint64_t(slot.slot),
		&item_key,
		C.int64_t(when),
		&cnt)
	if int(ret) == 0 {
		return int64(cnt), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) PTTL(key []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	left := C.int64_t(0)
	ret := C.fdb_pexpire_left(slot.fdb.ctx,
		C.uint64_t(slot.slot),
		&item_key,
		&left)
	if int(ret) == 0 {
		return int64(left), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}

func (slot *FdbSlot) Type(key []byte) ([]byte, error) {
	return []byte(""), nil
}

func (slot *FdbSlot) Persist(key []byte) (int64, error) {
	lock := slot.fetchKeysLock(string(key))
	lock.acquire()
	defer lock.release()

	var item_key C.fdb_item_t
	item_key.data_ = (*C.char)(unsafe.Pointer(&key[0]))
	item_key.data_len_ = C.uint64_t(len(key))

	cnt := C.int64_t(0)
	ret := C.fdb_pexpire_persist(slot.fdb.ctx,
		C.uint64_t(slot.slot),
		&item_key,
		&cnt)

	if int(ret) == 0 {
		return int64(cnt), nil
	}
	return 0, &FdbError{retcode: int(ret)}
}
