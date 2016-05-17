package fdb

import (
	"bytes"
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
}
