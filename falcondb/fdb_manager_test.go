package fdb

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
)

var fdb *FdbManager
var _err error

func GetFdb() (*FdbManager, error) {
	if fdb == nil {
		fdb = NewFdbManager()
		dbname := "/tmp/for_test_fdb"
		fdb.DropDB(dbname)
		_err = fdb.InitDB(dbname, 128, 128, 10)
	}
	return fdb, _err
}

func TestString(t *testing.T) {
	fdb, _err := GetFdb()
	if _err != nil {
		t.Fatalf("newFdbManager error %s\n", _err.Error())
	}
	slot := fdb.GetFdbSlot(5)

	key1 := []byte("key1")
	val1 := []byte("val1")

	err1 := slot.Set(key1, val1)
	if err1 != nil {
		t.Errorf("Set key1 %s val1 %s err, ret %d", string(key1), string(val1), err1.(*FdbError).Code())
	}

	key2 := key1
	val2, err2 := slot.Get(key2)
	if err2 != nil {
		t.Errorf("Get key2 %s val2 %s err, ret %d", string(key2), string(val2), err2.(*FdbError).Code())
	}

	if bytes.Compare(val1, val2) != 0 {
		t.Errorf("val1 %s  val2 %s  They don't equal to each other", string(val1), string(val2))
	}

	cnt, err3 := slot.Del(key1)
	if err3 != nil {
		t.Errorf("Del key1 %s %s err, ret %d", string(key1), err3.(*FdbError).Code())
	}
	t.Logf("Del key1 %s, %lld", string(key1), cnt)

	val4, err4 := slot.Get(key1)
	if err4 == nil {
		t.Errorf("Get key1 %s val4 %s ", string(key1), string(val4))
	}

	kvs := make([]FdbPair, 3)
	for i := 0; i < 3; i++ {
		ind := strconv.Itoa(i)

		key := "key" + ind
		val := "val" + ind
		kvs[i].Key = []byte(key)
		kvs[i].Value = []byte(val)
	}
	rets5, err5 := slot.MSet(kvs)
	if err5 != nil {
		t.Errorf("Get kvs %s err, rets5 %s, ret %d", kvs, rets5, err5.(*FdbError).Code())
	}

	for i := 0; i < len(rets5); i++ {
		if rets5[i] != nil {
			t.Logf("MSet rets5[%d] ret %d", i, rets5[i].(*FdbError).Code())
		}
	}

	keys6 := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		ind := strconv.Itoa(i)

		key := "key" + ind
		keys6[i] = []byte(key)
	}

	rets6, err6 := slot.MGet(keys6...)
	if err6 != nil {
		t.Errorf("MGet keys %s err, rets6 %s, ret %d", keys6, rets6, err6.(*FdbError).Code())
	}
	for i := 0; i < 4; {
		ind := strconv.Itoa(i)

		val := "val" + ind
		if bytes.Compare([]byte(val), rets6[i]) != 0 {
			t.Logf("MGet rets6[%d] %s,  val %s", i, rets6[i], string(val))
		}
		i++
	}
}

func TestHash(t *testing.T) {
	fdb, _err := GetFdb()
	if _err != nil {
		t.Fatalf("newFdbManager error %s\n", _err.Error())
	}
	slot := fdb.GetFdbSlot(5)
	key := []byte("hkey")
	{
		fld := []byte("hfld1")
		val := []byte("hval1")

		cnt, err := slot.HSet(key, fld, val)
		if err != nil {
			t.Errorf("HSet key %s field %s value %s  err %d", key, fld, val, err.(*FdbError).Code())
		} else {
			t.Logf("HSet cnt %d", cnt)
		}
	}

	{
		fld := []byte("hfld1")
		val, err := slot.HGet(key, fld)
		if err != nil {
			t.Errorf("HGet key %s field %s  err %d", key, fld, err.(*FdbError).Code())
		} else {
			t.Logf("HGet val %s", val)
		}

		fld_not_exists := []byte("hfld_not_exists")
		_, err_not_exists := slot.HGet(key, fld_not_exists)
		if err_not_exists != nil {
			t.Logf("HGet key %s field %s code %d", key, fld_not_exists, err_not_exists.(*FdbError).Code())
		}

	}

	{
		flds := [...][]byte{[]byte("hfld1"), []byte("hfld2"), []byte("hfld3"), []byte("hfld4")}
		vals := [...][]byte{[]byte("hfld1"), []byte("hval2"), []byte("hval3"), []byte("hval4")}
		cnt, err := slot.HMset(key, flds[:], vals[:])
		if err != nil {
			t.Errorf("HMset key %s fields %s values %s err %d", key, flds, vals, err.(*FdbError).Code())
		} else {
			if cnt != 3 {
				t.Errorf("HMset key %s cnt %d expect %d", key, cnt, 3)
			}
		}
	}

	{
		expects := [...][]byte{[]byte("hfld1"), []byte("hval2"), []byte("hval3"), []byte("hval4")}

		flds := [...][]byte{[]byte("hfld1"), []byte("hfld2"), []byte("hfld3"), []byte("hfld4")}
		vals, err := slot.HMget(key, flds[:]...)
		if err != nil {
			t.Errorf("HMget key %s fields %s  err %d", key, flds, err.(*FdbError).Code())
		} else {
			for i := 0; i < len(expects); i++ {
				if bytes.Compare(expects[i], vals[i]) != 0 {
					t.Errorf("HMget key %s expect %s != value %s", key, expects[i], vals[i])
				}
			}
		}
	}

	{
		flds := [...][]byte{[]byte("hfld3"), []byte("hfld4")}
		cnt, err := slot.HDel(key, flds[:]...)
		if err != nil {
			t.Errorf("HDel key %s fields %s  err %d", key, flds, err.(*FdbError).Code())
		} else {
			if cnt != 2 {
				t.Errorf("HDel key %s cnt %d expect %d", key, cnt, 2)
			}
		}
	}

	{
		length, err := slot.HLen(key)
		if err != nil {
			t.Errorf("HLen key %s  err %d", key, err.(*FdbError).Code())
		} else {
			if length != 2 {
				t.Errorf("HLen %s length %d expect %d", key, length, 2)
			}
		}
	}

	{
		fld := []byte("hfld5")
		_, _ = slot.HIncrBy(key, fld, 89)
		val, err := slot.HIncrBy(key, fld, -10)
		if err != nil {
			t.Errorf("HIncrBy key %s field %d err %d", key, fld, err.(*FdbError).Code())
		} else {
			if val != 79 {
				t.Errorf("HIncrBy key %s val %d expect %d", key, val, 79)
			}
		}
	}

}

func BenchmarkSet(b *testing.B) {
	fdb, _err := GetFdb()
	if _err != nil {
		panic("newFdbManager error " + _err.Error())
	}
	slot := fdb.GetFdbSlot(5)
	fmt.Printf("slot ID %llu", slot.slot)

	for i := 0; i < 1000; i += 1 {
		for j := 0; j < 10000; j += 1 {
			iVal := j + 1
			iKey := j + 1
			val := []byte(strconv.Itoa(iVal))
			key := []byte(strconv.Itoa(iKey))
			err := slot.Set(val, key)
			if err != nil {
				fmt.Printf("Set key %s val %s err, ret %d", string(key), string(val), err.(*FdbError).Code())
			}
		}
	}
}
