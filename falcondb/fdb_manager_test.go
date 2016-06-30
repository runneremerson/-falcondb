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

func TestZSet(t *testing.T) {
	fdb, _err := GetFdb()
	if _err != nil {
		t.Fatalf("newFdbManager error %s\n", _err.Error())
	}
	slot := fdb.GetFdbSlot(5)
	key := []byte("ZSetkey") //ZAdd key member score
	{
		var members [][]byte
		members = append(members, []byte("member1"))
		members = append(members, []byte("member2"))
		members = append(members, []byte("member3"))
		members = append(members, []byte("member4"))

		var scores []float64
		scores = append(scores, 1.0)
		scores = append(scores, 2.0)
		scores = append(scores, 3.0)
		scores = append(scores, 4.0)

		cnt, err := slot.ZCard(key)
		if err == nil {
			if cnt != 0 {
				t.Errorf("ZCard key %s  cnt  %d", key, cnt)
			}
		} else {
			t.Errorf("ZCard key %s  err %d", key, err.(*FdbError).Code())
		}

		cnt, err = slot.ZAdd(key, members, scores)
		if err != nil {
			t.Errorf("ZAdd key %s members %v scores %v  err %d", key, members, scores, err.(*FdbError).Code())
		} else {
			if cnt != 4 {
				t.Errorf("ZAdd key %s, members %s, scores %s , has added 4 nodes", members, scores)
			}
		}
	}

	{

		cnt, err := slot.ZCard(key)
		if err != nil {
			t.Errorf("ZCard key %s  err %d", key, err.(*FdbError).Code())
		} else {
			if cnt != 4 {
				t.Errorf("ZCard key %s  cnt %d, cnt should be 4 .", key, cnt)
			}
		}
		key := []byte("ZSetkey1")
		cnt, err = slot.ZCard(key)
		if err != nil {
			if err.(*FdbError).Code() != 3 {
				t.Errorf("ZCard key %s  err %d", key, err.(*FdbError).Code())
			}
		} else {
			if cnt != 0 {
				t.Errorf("ZCard key %s  cnt %d, cnt should be 0 .", key, cnt)
			}
		}
	}

	{

		var score float64
		member := []byte("member2")
		score, err := slot.ZScore(key, member)
		if err != nil {
			t.Errorf("ZScore key %s  member %s err %d", key, member, err.(*FdbError).Code())
		} else {
			if score != 2.0 {
				t.Errorf("ZScore key %s  member %s score %0.1f,score should be 2.0 .", key, member, score)
			}
		}
	}

	{

		member := []byte("member1")
		cnt, err := slot.ZRem(key, member)
		if err != nil {
			t.Errorf("ZRem key %s  member  %s  err %d", key, member, err.(*FdbError).Code())
		} else {
			if cnt != 1 {
				t.Errorf("ZRem key %s member %s cnt %d, cnt should be 1 .", key, member, cnt)
			}
		}
		cnt, err = slot.ZRank(key, member)
		if err == nil {
			t.Errorf("ZRank key %s  member  %s offset %d", key, member, cnt)
		} else {
			if cnt != 0 && err.(*FdbError).Code() != 3 {
				t.Errorf("ZRank key %s  member  %s  err %d, err should be 3 .", key, member, err.(*FdbError).Code())
			}
		}
		key := []byte("ZSetkeyTest")
		cnt, err = slot.ZRank(key, member)
		if err == nil {
			t.Errorf("ZRank key %s  member  %s offset %d", key, member, cnt)
		} else {
			if cnt != 0 && err.(*FdbError).Code() != 3 {
				t.Errorf("ZRank key %s  member  %s  err %d, err should be 3 .", key, member, err.(*FdbError).Code())
			}
		}
	}

	{
		member := []byte("member1")
		cnt, err := slot.ZRank(key, member)
		if err == nil {
			t.Errorf("ZRank key %s  member  %s offset %d", key, member, cnt)
		} else {
			if err.(*FdbError).Code() != 3 && cnt != 0 {
				t.Errorf("ZRank key %s  member  %s err %d, err should be 3 .", key, member, err.(*FdbError).Code())
			}
		}

		member = []byte("member4")
		cnt, err = slot.ZRank(key, member)
		if err == nil {
			if cnt != 2 {
				t.Errorf("ZRank key %s  member  %s offset %d, the offset should be 2 .", key, member, cnt)
			}
		} else {
			t.Errorf("ZRank key %s  member  %s err %d .", key, member, err.(*FdbError).Code())
		}

		member = []byte("member2")
		cnt, err = slot.ZRank(key, member)
		if err == nil {
			if cnt != 0 {
				t.Errorf("ZRank key %s  member  %s offset %d, the offset should be 0 .", key, member, cnt)
			}
		} else {
			t.Errorf("ZRank key %s  member  member2 err %d .", key, err.(*FdbError).Code())
		}
	}

	{
		key := []byte("ZSetkey1")

		zr, zs, err := slot.ZRange(key, 0, 2, true)
		if err != nil {
			t.Errorf("ZRange key %s  err %d .", key, err.(*FdbError).Code())
		} else {
			if len(zr) != 0 {
				t.Errorf("ZRange key %s  zr is %s .", key, zr)
			}
		}

		key = []byte("ZSetkey")
		member := []byte("member2")
		cnt, err := slot.ZRem(key, member)
		if err != nil {
			t.Errorf("ZRem key %s  member  %s  err %d", key, member, err.(*FdbError).Code())
		} else {
			if cnt != 1 {
				t.Errorf("ZRem key %s member %s cnt %d, cnt should be 1 .", key, member, cnt)
			}
		}

		zr, zs, err = slot.ZRange(key, 0, 1, false)
		if err != nil {
			t.Errorf("ZRange key %s  err %d .", key, err.(*FdbError).Code())
		} else {
			if len(zr) != len(zs) {
				t.Errorf("ZRange key %s  members %s, withscore error, start 0, stop 1.", key, zr)
			}
		}
		zr, zs, err = slot.ZRange(key, 0, 2, true)
		if err != nil {
			t.Errorf("ZRange key %s  err %d .", key, err.(*FdbError).Code())
		} else {
			if len(zr) != len(zs) {
				t.Errorf("ZRange key %s  withscore error,  tart 2, stop 3.", key)
			}
		}

	}

	{
		cnt, err := slot.ZCount(key, 1.0, 5.0, 1)
		if err != nil {
			t.Errorf("ZCount key %s from  1.0 to 5.0 type 1  error.", key)
		} else {
			if cnt != 2 {
				t.Errorf("ZCount key %s  from  1.0 to 5.0 type 1 cnt is %d. ", key, cnt)
			}
		}

		var members [][]byte
		var scores []float64
		members = append(members, []byte("member1"))
		members = append(members, []byte("member2"))
		members = append(members, []byte("member3"))
		members = append(members, []byte("member4"))
		members = append(members, []byte("member5"))

		scores = append(scores, 5.0)
		scores = append(scores, 2.0)
		scores = append(scores, 5.0)
		scores = append(scores, 4.0)
		scores = append(scores, 1.0)
		cnt, err = slot.ZAdd(key, members, scores)
		if err != nil {
			t.Errorf("ZAdd key %s members %s scores %v  err %d", key, members, scores, err.(*FdbError).Code())
		} else {
			if cnt != 3 {
				t.Errorf("ZAdd key %s members %s scores %v  cnt %d, cnt should be 3.", key, members, scores, cnt)
			}
		}
		cnt, err = slot.ZCount(key, 1.0, 5.0, 1)
		if err != nil {
			t.Errorf("ZCount key %s from  1.0 to 5.0 type 1  error.", key)
		} else {
			if cnt != 4 {
				t.Errorf("ZCount key %s  from  1.0 to 5.0 type 1 cnt is %d. ", key, cnt)
			}
		}
		cnt, err = slot.ZCount(key, 1.0, 5.0, 2)
		if err != nil {
			t.Errorf("ZCount key %s from  1.0 to 5.0 type 2  error.", key)
		} else {
			if cnt != 3 {
				t.Errorf("ZCount key %s  from  1.0 to 5.0 type 2 cnt is %d. ", key, cnt)
			}
		}
		cnt, err = slot.ZCount(key, 1.0, 5.0, 3)
		if err != nil {
			t.Errorf("ZCount key %s from  1.0 to 5.0 type 3  error.", key)
		} else {
			if cnt != 2 {
				t.Errorf("ZCount key %s  from  1.0 to 5.0 type 3 cnt is %d. ", key, cnt)
			}
		}
	}

	{
		var members [][]byte
		var scores []float64
		members = append(members, []byte("member1"))
		members = append(members, []byte("member2"))
		members = append(members, []byte("member3"))
		members = append(members, []byte("member4"))
		members = append(members, []byte("member5"))

		scores = append(scores, 5.0)
		scores = append(scores, 2.0)
		scores = append(scores, 5.0)
		scores = append(scores, 4.0)
		scores = append(scores, 6.0)
		zr, zs, err := slot.ZRange(key, 0, 7, true)
		if err != nil {
			t.Errorf("ZRange key %s  err %d .", key, err.(*FdbError).Code())
		} else {
			if string(zr[0]) != "member5" || zs[0] != 1 ||
				string(zr[1]) != "member2" || zs[1] != 2 ||
				string(zr[2]) != "member4" || zs[2] != 4 ||
				string(zr[3]) != "member1" || zs[3] != 5 ||
				string(zr[4]) != "member3" || zs[4] != 5 {
				t.Errorf("ZRange key %s, members %s, scores %v", key, zr, zs)
			}
		}

		cnt, err := slot.ZRemRangeByRank(key, 0, 1)
		if err != nil {
			t.Errorf("ZRemRangeByRank key %s, members %s, err %d", key, members, err.(*FdbError).Code())
		} else {
			if cnt != 2 {
				t.Errorf("ZRemRangeByRank key %s, members %s, cnt %d", key, members, cnt)
			}
		}
		zr, zs, err = slot.ZRange(key, 0, 7, true)
		if err != nil {
			t.Errorf("ZRange key %s  err %d .", key, err.(*FdbError).Code())
		} else {
			if len(zr) != 3 {
				t.Errorf("ZRange key %s, members %s, scores %v", key, zr, zs)
			}
			if string(zr[0]) != "member4" || zs[0] != 4 ||
				string(zr[1]) != "member1" || zs[1] != 5 ||
				string(zr[2]) != "member3" || zs[2] != 5 {
				t.Errorf("ZRemRangeByRank key %s, start 2, stop 1, err", key)
			}
		}

		member := []byte("member1")
		score, err := slot.ZIncrBy(key, 3.0, member)
		if err != nil {
			t.Errorf("ZIncrBy key %s, member %s, err %d", key, member, err.(*FdbError).Code())
		} else if score != 8.0 {
			t.Errorf("ZIncrBy key %s, member %s, score %0.1f", key, member, score)
		}

		member = []byte("member5")
		score, err = slot.ZIncrBy(key, 3.0, member)
		if err != nil {
			t.Errorf("ZIncrBy key %s, member %s, err %d", key, member, err.(*FdbError).Code())
		} else if score != 3.0 {
			t.Errorf("ZIncrBy key %s, member %s, score %0.1f", key, member, score)
		}

		cnt, err = slot.ZRem(key, members...)
		if err != nil {
			t.Errorf("ZRem key %s, members %s, err %d", key, members, err.(*FdbError).Code())
		} else {
			if cnt != 4 {
				t.Errorf("ZRem key %s, members %s, cnt %d", key, members, cnt)
			}
		}

		zr, zs, err = slot.ZRange(key, 0, 7, true)
		if err != nil {
			t.Errorf("ZRange key %s  err %d .", key, err.(*FdbError).Code())
		} else {
			if len(zr) == len(zs) {
				if len(zr) != 0 {
					t.Errorf("ZRem key %s  member %s  scores %v.", key, members, scores)
				}
			}
		}

	}

	{
		var members [][]byte
		var scores []float64
		members = append(members, []byte("member1"))
		members = append(members, []byte("member2"))
		members = append(members, []byte("member3"))
		members = append(members, []byte("member4"))
		members = append(members, []byte("member5"))

		scores = append(scores, 5.0)
		scores = append(scores, 2.0)
		scores = append(scores, 5.0)
		scores = append(scores, 4.0)
		scores = append(scores, 6.0)
		cnt, err := slot.ZAdd(key, members, scores)
		if err != nil {
			t.Errorf("ZAdd key %s members %s scores %v  err %d", key, members, scores, err.(*FdbError).Code())
		} else {
			if cnt != 5 {
				t.Errorf("ZAdd key %s members %s scores %v  cnt %d, cnt should be 5.", key, members, scores, cnt)
			}
		}

		zr, zs, err := slot.ZRange(key, 0, 5, true)
		if err != nil {
			t.Errorf("ZRange key %s start 0, stop 5", key, err.(*FdbError).Code())
		} else {
			if string(zr[0]) != "member2" || zs[0] != 2.0 ||
				string(zr[1]) != "member4" || zs[1] != 4.0 ||
				string(zr[2]) != "member1" || zs[2] != 5.0 ||
				string(zr[3]) != "member3" || zs[3] != 5.0 ||
				string(zr[4]) != "member5" || zs[4] != 6.0 {
				t.Errorf("ZRange key %s start 0, stop 5, member %s, score %v", key, zr, zs)
			}
		}
		zr, zs, err = slot.ZRevRange(key, 0, 5, true)
		if err != nil {
			t.Errorf("ZRange key %s start 0, stop 5", key, err.(*FdbError).Code())
		} else {
			if string(zr[0]) != "member5" || zs[0] != 6.0 ||
				string(zr[1]) != "member3" || zs[1] != 5.0 ||
				string(zr[2]) != "member1" || zs[2] != 5.0 ||
				string(zr[3]) != "member4" || zs[3] != 4.0 ||
				string(zr[4]) != "member2" || zs[4] != 2.0 {
				t.Errorf("ZRange key %s start 0, stop 5, member %s, score %v", key, zr, zs)
			}
		}

		cnt, err = slot.ZRemRangeByScore(key, 1.0, 2.0, 1)
		if err != nil {
			t.Errorf("ZRemRangeByScore key %s delete members when scores are in 1.0 between 2.0.", key)
		} else {
			if cnt != 1 {
				t.Errorf("ZRemRangeByScore key %s delete members when scores are in 1.0 between 2.0 , cnt %d.", key, cnt)
			}
		}
		zr, zs, err = slot.ZRange(key, 0, 5, true)
		if err != nil {
			t.Errorf("ZRange key %s start 0, stop 5", key, err.(*FdbError).Code())
		} else {
			t.Logf("zr is %s, zs %v", zr, zs)
		}
		cnt, err = slot.ZRemRangeByScore(key, 1.0, 2.0, 1)
		if err != nil {
			t.Errorf("ZRemRangeByScore key %s delete members when scores are in 1.0 between 2.0.", key)
		} else {
			if cnt != 0 {
				t.Errorf("ZRemRangeByScore key %s delete members when scores are in 1.0 between 2.0 , cnt %d.", key, cnt)
			}
		}
		zr, zs, err = slot.ZRange(key, 0, 5, true)
		if err != nil {
			t.Errorf("ZRange key %s start 0, stop 5", key, err.(*FdbError).Code())
		} else {
			t.Logf("zr is %s, zs is %v", zr, zs)
		}
		cnt, err = slot.ZAdd(key, members, scores)
		if err != nil {
			t.Errorf("ZAdd key %s members %s scores %v  err %d", key, members, scores, err.(*FdbError).Code())
		} else {
			if cnt != 2 {
				t.Errorf("ZAdd key %s members %s scores %v  cnt %d, cnt should be 2.", key, members, scores, cnt)
			}
		}
		cnt, err = slot.ZRemRangeByScore(key, 1.0, 2.0, 1)
		if err != nil {
			t.Errorf("ZRemRangeByScore key %s delete members when scores are in 1.0 between 2.0.", key)
		} else {
			if cnt != 0 {
				t.Errorf("ZRemRangeByScore key %s delete members when scores are in 1.0 between 2.0 , cnt %d.", key, cnt)
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
