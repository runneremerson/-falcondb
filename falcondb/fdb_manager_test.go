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

	{
		key := []byte("key1")
		val := []byte("val1")

		err := slot.Set(key, val)
		if err != nil {
			t.Errorf("Set key %s val %s err, err %d", string(key), string(val), err.(*FdbError).Code())
		}
	}

	{
		key := []byte("key1")
		expect := []byte("val1")
		val, err := slot.Get(key)
		if err != nil {
			t.Errorf("Get key %s val %s err, err %d", string(key), string(val), err.(*FdbError).Code())
		}

		if bytes.Compare(val, expect) != 0 {
			t.Errorf("val %s  expect %s  They don't equal to each other", string(val), string(expect))
		}
	}

	{
		key := []byte("key1")
		cnt, err := slot.Del(key)
		if err != nil {
			t.Errorf("Del key %s %s err, err %d", string(key), err.(*FdbError).Code())
		}
		t.Logf("Del key %s, %lld", string(key), cnt)
	}

	{
		key := []byte("key1")
		val, err := slot.Get(key)
		if err != nil {
			t.Errorf("Get key %s val %s ", string(key), string(val))
		}
	}

	{
		kvs := make([]FdbPair, 3)
		for i := 0; i < 3; i++ {
			ind := strconv.Itoa(i)

			key := "key" + ind
			val := "val" + ind
			kvs[i].Key = []byte(key)
			kvs[i].Value = []byte(val)
		}
		rets, err := slot.MSet(kvs)
		if err != nil {
			t.Errorf("MSet kvs %s rets %s, err %d", kvs, rets, err.(*FdbError).Code())
		}

		for i := 0; i < len(rets); i++ {
			if rets[i] != nil {
				t.Logf("MSet rets[%d] ret %d", i, rets[i].(*FdbError).Code())
			}
		}
	}

	{
		keys := make([][]byte, 4)
		for i := 0; i < 4; i++ {
			ind := strconv.Itoa(i)

			key := "key" + ind
			keys[i] = []byte(key)
		}

		rets, err := slot.MGet(keys...)
		if err != nil {
			t.Errorf("MGet keys %s err, rets %s, ret %d", keys, rets, err.(*FdbError).Code())
		}
		for i := 0; i < 4; {
			ind := strconv.Itoa(i)

			val := "val" + ind
			if bytes.Compare([]byte(val), rets[i]) != 0 {
				t.Logf("MGet rets[%d] %s,  val %s", i, rets[i], string(val))
			}
			i++
		}
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
		expect_flds := [...][]byte{[]byte("hfld1"), []byte("hfld2"), []byte("hfld3"), []byte("hfld4")}
		expect_vals := [...][]byte{[]byte("hfld1"), []byte("hval2"), []byte("hval3"), []byte("hval4")}
		flds, vals, err := slot.HGetAll(key)
		if err != nil {
			t.Errorf("HGetAll key %s err %d", key, err.(*FdbError).Code())
		} else {
			for i := 0; i < len(expect_flds); i++ {
				if bytes.Compare(expect_flds[i], flds[i]) != 0 {
					t.Errorf("HGetAll key %s expect field %s != field %s", key, expect_flds[i], flds[i])
				}

				if bytes.Compare(expect_vals[i], vals[i]) != 0 {
					t.Errorf("HGetAll key %s expect value %s != value %s", key, expect_vals[i], vals[i])
				}
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
		not_exists_key := []byte("not_exists_hkey")
		length, err := slot.HLen(not_exists_key)
		if err != nil {
			t.Errorf("HLen not_exists_key %s  err %d", not_exists_key, err.(*FdbError).Code())
		} else {
			if length != 0 {
				t.Errorf("HLen %s length %d expect %d", key, length, 0)
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

	{
		fld_exists := []byte("hfld5")
		cnt_exists, err_exists := slot.HExists(key, fld_exists)
		if err_exists != nil {
			t.Errorf("HExists key %s field %d err %d", key, fld_exists, err_exists.(*FdbError).Code())
		} else {
			if cnt_exists != 1 {
				t.Errorf("HExists key %s cnt %d expect %d", key, cnt_exists, 1)
			}
		}

		fld_not_exists := []byte("hfld6")
		cnt_not_exists, err_not_exists := slot.HExists(key, fld_not_exists)
		if err_not_exists != nil {
			t.Errorf("HExists key %s field %d err %d", key, fld_not_exists, err_not_exists.(*FdbError).Code())
		} else {
			if cnt_not_exists != 0 {
				t.Errorf("HExists key %s cnt %d expect %d", key, cnt_not_exists, 0)
			}
		}

	}

	{
		fld := []byte("hfld5")
		val := []byte("hval5")

		cnt, err := slot.HSetNX(key, fld, val)
		if err == nil {
			t.Errorf("HSetNX key %s field %s value %s cnt %d", key, fld, val, cnt)
		} else {
			t.Logf("HSetNX key %s field %s value %s err %d", key, fld, val, err.(*FdbError).Code())
		}
	}

	{
		fld := []byte("hfld6")
		val := []byte("hval6")

		cnt, err := slot.HSetNX(key, fld, val)
		if err != nil {
			t.Errorf("HSetNX key %s field %s value %s err %d", key, fld, val, err.(*FdbError).Code())
		} else {
			t.Logf("HSetNX key %s field %s value %s cnt %d", key, fld, val, cnt)
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
