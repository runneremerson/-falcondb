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

	err1 := slot.Set(key1, val1, 0, 1)
	if err1.(*FdbError).Code() != 0 {
		t.Errorf("Get key1 %s val1 %s err, ret %d", string(key1), string(val1), err1.(*FdbError).Code())
	}

	key2 := key1
	val2, err2 := slot.Get(key2)
	if err2.(*FdbError).Code() != 0 {
		t.Errorf("Get key2 %s val2 %s err, ret %d", string(key2), string(val2), err2.(*FdbError).Code())
	}

	if bytes.Compare(val1, val2) != 0 {
		t.Errorf("val1 %s  val2 %s  They don't equal to each other", string(val1), string(val2))
	}

	err3 := slot.Del(key1)
	if err3.(*FdbError).Code() != 0 {
		t.Errorf("Del key1 %s %s err, ret %d", string(key1), err3.(*FdbError).Code())
	}

	val4, err4 := slot.Get(key1)
	if err4.(*FdbError).Code() != 3 {
		t.Errorf("Get key1 %s val4 %s err, ret %d", string(key1), string(val4), err4.(*FdbError).Code())
	}

	kvs := make([]FdbValue, 6)
	j := 0
	for i := 0; i < 3; i++ {
		ind := strconv.Itoa(i)

		key := "key" + ind
		kvs[j].val = []byte(key)
		kvs[j].ret = 0
		j++
		val := "val" + ind
		kvs[j].val = []byte(val)
		kvs[j].ret = 0
		j++
	}
	rets5, err5 := slot.MSet(kvs, 1)
	if err5.(*FdbError).Code() != 0 {
		t.Errorf("Get kvs %s err, rets5 %s, ret %d", kvs, rets5, err5.(*FdbError).Code())
	}

	for i := 0; i < len(rets5); i++ {
		if rets5[i].ret != 0 {
			t.Logf("MSet rets5[%d] ret %d", i, rets5[i].ret)
		}
	}

	keys := make([]FdbValue, 4)
	for i := 0; i < 4; {
		ind := strconv.Itoa(i)

		key := "key" + ind
		keys[i].val = []byte(key)
		keys[i].ret = 0
		i++
	}

	rets6, err6 := slot.MGet(keys)
	if err6.(*FdbError).Code() != 0 {
		t.Errorf("MGet keys %s err, rets6 %s, ret %d", keys, rets6, err6.(*FdbError).Code())
	}
	for i := 0; i < 4; {
		ind := strconv.Itoa(i)

		val := "val" + ind
		if bytes.Compare([]byte(val), rets6[i].val) != 0 {
			t.Logf("MSet rets6[%d].val %s, ret %d, val %s", i, string(rets6[i].val), rets6[i].ret, string(val))
		}
		i++
	}

}
