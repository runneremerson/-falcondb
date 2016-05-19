package fdb

import (
	"bytes"
	_ "fmt"
	"strconv"
	"testing"
)

var fdb *FdbManager
var _err error

func GetFdb() (*FdbManager, error) {
	if fdb == nil {
		fdb, _ = NewFdbManager()
		_err = fdb.InitDB("/tmp/for_test_fdb", 128, 128, 10)
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
		t.Errorf("Get key1 %s val1 %s err, ret %d", string(key1), string(val1), err1.(*FdbError).Code())
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
		kvs[i].Val = []byte(val)
		kvs[i].Ret = nil
	}
	rets5, err5 := slot.MSet(kvs)
	if err5 != nil {
		t.Errorf("Get kvs %s err, rets5 %s, ret %d", kvs, rets5, err5.(*FdbError).Code())
	}

	for i := 0; i < len(rets5); i++ {
		if rets5[i].Ret != nil {
			t.Logf("MSet rets5[%d] ret %d", i, rets5[i].Ret.(*FdbError).Code())
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
		if bytes.Compare([]byte(val), rets6[i].Val) != 0 {
			t.Logf("MGet rets6[%d] %s,  val %s", i, rets6[i], string(val))
		}
		i++
	}

}
