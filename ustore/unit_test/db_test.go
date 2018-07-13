package ustoredb

import (
	// "strings"
	"fmt"
	"testing"
	"ustore"
)

func TestKVDB_State(t *testing.T) {
	/* Construct the following lineage graph:
	   x: x0 -> x1 -> x2
	   y: y0 -> y1 -> y2 -> y3 -> y4 -> y5 -> y6 -> y7
	   z: z0
	*/

	kvdb := ustore.NewKVDB()
	stat := kvdb.InitGlobalState()
	if !stat.Ok() {
		t.Error("Fail to init global state")
	}

	kx := "x"
	ky := "y"
	kz := "z"

	deps := ustore.NewVecStr()

	// Dependency:
	// 1) -(tx0)-> x0
	// 2) -(ty0)-> y0
	// 3) -(tz0)-> z0
	fmt.Println("=====================Block 0=====================")
	if !kvdb.PutState(kx, "x0", "tx0", deps) {
		t.Error("Fail to Put x0")
	}
	if !kvdb.PutState(ky, "y0", "ty0", deps) {
		t.Error("Fail to Put y0")
	}
	if !kvdb.PutState(kz, "z0", "tz0", deps) {
		t.Error("Fail to Put z0")
	}

	if status_version := kvdb.Commit(); !status_version.GetFirst().Ok() {
		t.Error("Fail to commit Block 0")
	}

	// 4) x0, y0 -(tx1)-> x1
	// 5) y0 -(ty1)-> y1
	fmt.Println("=====================Block 1=====================")
	deps.Clear()
	deps.Add(kx)
	deps.Add(ky)
	if !kvdb.PutState(kx, "x1", "tx1", deps) {
		t.Error("Fail to Put x1")
	}

	deps.Clear()
	deps.Add(ky)
	if !kvdb.PutState(ky, "y1", "ty1", deps) {
		t.Error("Fail to Put y1")
	}
	if status_version := kvdb.Commit(); !status_version.GetFirst().Ok() {
		t.Error("Fail to commit Block 1")
	}

	// 6) x1, z0 -(tx2)-> x2
	// 7) x2, y1, z0 -(ty2)-> y2
	fmt.Println("=====================Block 2=====================")
	deps.Clear()
	deps.Add(kx)
	deps.Add(kz)
	if !kvdb.PutState(kx, "x2", "tx2", deps) {
		t.Error("Fail to Put x2")
	}

	deps.Clear()
	deps.Add(kx)
	deps.Add(ky)
	deps.Add(kz)
	if !kvdb.PutState(ky, "y2", "ty2", deps) {
		t.Error("Fail to Put y2")
	}
	if status_version := kvdb.Commit(); !status_version.GetFirst().Ok() {
		t.Error("Fail to commit Block 2")
	}

	// 8) y2 -(ty3) -> y3
	fmt.Println("=====================Block 3====================")
	deps.Clear()
	deps.Add(ky)
	if !kvdb.PutState(ky, "y3", "ty3", deps) {
		t.Error("Fail to Put y3")
	}
	if status_version := kvdb.Commit(); !status_version.GetFirst().Ok() {
		t.Error("Fail to commit Block 3")
	}
	// 9) y3 -(ty4) -> y4
	fmt.Println("=====================Block 4====================")
	deps.Clear()
	deps.Add(ky)
	if !kvdb.PutState(ky, "y4", "ty4", deps) {
		t.Error("Fail to Put y4")
	}
	if status_version := kvdb.Commit(); !status_version.GetFirst().Ok() {
		t.Error("Fail to commit Block 4")
	}
	// 10) y4 -(ty5) -> y5
	fmt.Println("=====================Block 5====================")
	deps.Clear()
	deps.Add(ky)
	if !kvdb.PutState(ky, "y5", "ty5", deps) {
		t.Error("Fail to Put y5")
	}
	if status_version := kvdb.Commit(); !status_version.GetFirst().Ok() {
		t.Error("Fail to commit Block 5")
	}
	// 11) y5 -(ty6) -> y6
	fmt.Println("=====================Block 6====================")
	deps.Clear()
	deps.Add(ky)
	if !kvdb.PutState(ky, "y6", "ty6", deps) {
		t.Error("Fail to Put y6")
	}
	if status_version := kvdb.Commit(); !status_version.GetFirst().Ok() {
		t.Error("Fail to commit Block 6")
	}
	// 12) y6 -(ty7) -> y7
	fmt.Println("=====================Block 7====================")
	deps.Clear()
	deps.Add(ky)
	if !kvdb.PutState(ky, "y7", "ty7", deps) {
		t.Error("Fail to Put y7")
	}
	if status_version := kvdb.Commit(); !status_version.GetFirst().Ok() {
		t.Error("Fail to commit Block 7")
	}

	deps.Clear()
	// Valid Query
	fmt.Println("=====================Valid Query====================")
	status_state := kvdb.GetHistoricalState(ky, uint64(1))
	if !status_state.GetFirst().Ok() || status_state.GetSecond() != "y1" {
		t.Error("Fail to retrieve y1")
	}

	status_state = kvdb.GetHistoricalState(kz, uint64(3))
	if !status_state.GetFirst().Ok() || status_state.GetSecond() != "z0" {
		t.Error("Fail to retrieve z0")
	}

	status_txn := kvdb.GetTxnID(kx, uint64(0))
	if !status_txn.GetFirst().Ok() || status_txn.GetSecond() != "tx0" {
		t.Error("Fail to retrieve txn tx0")
	}

	status_deps := kvdb.GetDeps(ky, uint64(2))
	if !status_state.GetFirst().Ok() || status_deps.GetSecond().Size() != 3 {
		t.Error("Fail to get dependency for y2")
	}

	xid := status_deps.GetSecond().Get(kx)
	status_state = kvdb.GetHistoricalState(kx, xid)
	if !status_state.GetFirst().Ok() || status_state.GetSecond() != "x1" {
		t.Error("Fail to retrieve x1")
	}

	yid := status_deps.GetSecond().Get(ky)
	status_y_deps := kvdb.GetDeps(ky, yid)
	if !(status_deps.GetFirst().Ok() && status_y_deps.GetSecond().Size() == 1 && status_y_deps.GetSecond().Has_key(ky)) {
		t.Error("Fail to retrieve y dep.")
	}

	zid := status_deps.GetSecond().Get(kz)
	status_txn = kvdb.GetTxnID(kz, zid)
	if !status_txn.GetFirst().Ok() || status_txn.GetSecond() != "tz0" {
		t.Error("Fail to retrieve txn tz0")
	}

	// Invalid Query
	fmt.Println("=====================Invalid Query====================")
	status_state = kvdb.GetHistoricalState(ky, uint64(10))
	if status_state.GetFirst().Ok() {
		t.Error("Shouldnot be to retrieve y10")
	}

	status_state = kvdb.GetHistoricalState("fake", uint64(1))
	if status_state.GetFirst().Ok() {
		t.Error("Shouldnot be to retrieve fake key")
	}
}

// func TestKVDB_Ops(t *testing.T) {
// 	kvdb := ustore.NewKVDB()
// 	// put values
// 	st := kvdb.Put("key1", "val1")
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if !kvdb.Exist("key1") {
// 		t.Error("Expected key1 existed, but not.")
// 	}

// 	st = kvdb.Put("key2", "val2")
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if !kvdb.Exist("key2") {
// 		t.Error("Expected key3 existed, but not.")
// 	}

// 	st = kvdb.Put("key3", "val3")
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if !kvdb.Exist("key3") {
// 		t.Error("Expected key3 existed, but not.")
// 	}

// 	// get values
// 	ret := kvdb.Get("key1")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str := "val1"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	ret = kvdb.Get("key2")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str = "val2"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	ret = kvdb.Get("key3")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str = "val3"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	// get non-existing value
// 	ret = kvdb.Get("key4")
// 	if ret.GetFirst().Ok() {
// 		t.Errorf("Unexpected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if !ret.GetFirst().IsNotFound() {
// 		t.Errorf("Expected not_found status, but get: '%s'", ret.GetFirst().ToString())
// 	}

// 	// modify value
// 	expected_str = "22val22"
// 	st = kvdb.Put("key2", expected_str)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if !kvdb.Exist("key2") {
// 		t.Error("Expected key2 existed, but not.")
// 	}
// 	ret = kvdb.Get("key2")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	expected_str = "second time"
// 	st = kvdb.Put("key2", expected_str)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if !kvdb.Exist("key2") {
// 		t.Error("Expected key2 existed, but not.")
// 	}
// 	ret = kvdb.Get("key2")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	expected_str = "modify value 3"
// 	st = kvdb.Put("key3", expected_str)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if !kvdb.Exist("key3") {
// 		t.Error("Expected key3 existed, but not.")
// 	}
// 	ret = kvdb.Get("key3")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	expected_str = "value 2 third time"
// 	st = kvdb.Put("key2", expected_str)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if !kvdb.Exist("key2") {
// 		t.Error("Expected key2 existed, but not.")
// 	}
// 	ret = kvdb.Get("key2")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	// delete key
// 	st = kvdb.Delete("key1")
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if kvdb.Exist("key1") {
// 		t.Error("Expected key1 does not exist, but it exists.")
// 	}
// 	ret = kvdb.Get("key1")
// 	if ret.GetFirst().Ok() {
// 		t.Errorf("Unexpected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if !ret.GetFirst().IsNotFound() {
// 		t.Errorf("Expected not_found status, but get: '%s'", ret.GetFirst().ToString())
// 	}

// 	// insert the deleted key back
// 	expected_str = "key 1 is back"
// 	st = kvdb.Put("key1", expected_str)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if !kvdb.Exist("key1") {
// 		t.Error("Expected key2 existed, but not.")
// 	}
// 	ret = kvdb.Get("key1")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	// delete key again
// 	st = kvdb.Delete("key1")
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if kvdb.Exist("key1") {
// 		t.Error("Expected key1 does not exist, but it exists.")
// 	}
// 	ret = kvdb.Get("key1")
// 	if ret.GetFirst().Ok() {
// 		t.Errorf("Unexpected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if !ret.GetFirst().IsNotFound() {
// 		t.Errorf("Expected not_found status, but get: '%s'", ret.GetFirst().ToString())
// 	}

// 	// delete non-exist key
// 	st = kvdb.Delete("key5")
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}
// 	if kvdb.Exist("key5") {
// 		t.Error("Expected key1 does not exist, but it exists.")
// 	}
// 	ret = kvdb.Get("key5")
// 	if ret.GetFirst().Ok() {
// 		t.Errorf("Unexpected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if !ret.GetFirst().IsNotFound() {
// 		t.Errorf("Expected not_found status, but get: '%s'", ret.GetFirst().ToString())
// 	}

// 	ustore.DeleteKVDB(kvdb)
// }

// func put_helper(kvdb *UStoreDB, cfh *ColumnFamilyHandle, keys []string,
// 	values []string) error {
// 	for i, k := range keys {
// 		if err := kvdb.PutCF(cfh, k, values[i]); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func get_helper(kvdb *UStoreDB, cfh *ColumnFamilyHandle, keys []string) ([]string, error) {
// 	ret := make([]string, len(keys))
// 	for i, k := range keys {
// 		if val, err := kvdb.GetCF(cfh, k); err != nil {
// 			return nil, err
// 		} else {
// 			ret[i] = val
// 		}
// 	}
// 	return ret, nil
// }

// // test non-batch CF operatio
// func TestKVDB_ColumnFamily1(t *testing.T) {
// 	kvdb, err := OpenDB()
// 	defer Close(kvdb)
// 	if err != nil {
// 		t.Errorf("Error openning DB")
// 	}
// 	cfh1, _ := kvdb.CreateColumnFamily("state")
// 	cfh2, _ := kvdb.CreateColumnFamily("ledger")
// 	keys := []string{"key1", "key2", "key3"}
// 	vals := []string{"val1", "val2", "val3"}
// 	vals2 := []string{"val4", "val5", "val6"}
// 	if err := put_helper(kvdb, cfh1, keys, vals); err != nil {
// 		t.Errorf("Error during Put")
// 	}
// 	if err := put_helper(kvdb, cfh2, keys, vals2); err != nil {
// 		t.Errorf("Error during Put")
// 	}

// 	if vs, err := get_helper(kvdb, cfh1, keys); err != nil {
// 		t.Errorf("Error during Get")
// 	} else {
// 		for i, v := range vs {
// 			if v != vals[i] {
// 				t.Errorf("Wrong value, expect: %v, get: %v\n", vals[i], v)
// 			}
// 		}
// 	}

// 	if vs, err := get_helper(kvdb, cfh2, keys); err != nil {
// 		t.Errorf("Error during Get")
// 	} else {
// 		for i, v := range vs {
// 			if v != vals2[i] {
// 				t.Errorf("Wrong value, expect: %v, get: %v\n", vals2[i], v)
// 			}
// 		}
// 	}
// 	kvdb.GetCF(cfh1, "key3")
// 	//  fmt.Printf("Value for key3: %v\n", val)
// }

// func TestKVDB_ColumnFamilyBatch(t *testing.T) {
// 	kvdb, err := OpenDB()
// 	defer Close(kvdb)
// 	if err != nil {
// 		t.Errorf("Error openning DB")
// 	}
// 	cfh1, _ := kvdb.CreateColumnFamily("state")
// 	cfh2, _ := kvdb.CreateColumnFamily("ledger")
// 	keys := []string{"key1", "key2", "key3"}
// 	vals := []string{"val1", "val2", "val3"}
// 	vals2 := []string{"val4", "val5", "val6"}

// 	batch1, _ := NewWriteBatch()
// 	batch2, _ := NewWriteBatch()
// 	defer DeleteWriteBatch(batch1)
// 	defer DeleteWriteBatch(batch2)

// 	batch1.PutCF(cfh1, keys[0], vals[0])
// 	batch1.PutCF(cfh2, keys[0], vals2[0])
// 	batch2.PutCF(cfh1, keys[1], vals[1])
// 	batch2.PutCF(cfh2, keys[1], vals2[1])
// 	batch1.PutCF(cfh1, keys[2], vals[2])
// 	batch2.PutCF(cfh2, keys[2], vals2[2])

// 	kvdb.Write(batch1)
// 	kvdb.Write(batch2)

// 	if vs, err := get_helper(kvdb, cfh1, keys); err != nil {
// 		t.Errorf("Error during Get")
// 	} else {
// 		for i, v := range vs {
// 			if v != vals[i] {
// 				t.Errorf("Wrong value, expect: %v, get: %v\n", vals[i], v)
// 			}
// 		}
// 	}

// 	if vs, err := get_helper(kvdb, cfh2, keys); err != nil {
// 		t.Errorf("Error during Get")
// 	} else {
// 		for i, v := range vs {
// 			if v != vals2[i] {
// 				t.Errorf("Wrong value, expect: %v, get: %v\n", vals2[i], v)
// 			}
// 		}
// 	}
// }

// func TestKVDB_ColumnFamilyIterator(t *testing.T) {
// 	kvdb, err := OpenDB()
// 	defer Close(kvdb)
// 	if err != nil {
// 		t.Errorf("Error openning DB")
// 	}
// 	cfh1, _ := kvdb.CreateColumnFamily("state")
// 	cfh2, _ := kvdb.CreateColumnFamily("ledger")
// 	keys := []string{"key1", "key2", "key3"}
// 	vals := []string{"val1", "val2", "val3"}
// 	vals2 := []string{"val4", "val5", "val6"}

// 	batch1, _ := NewWriteBatch()
// 	batch2, _ := NewWriteBatch()
// 	defer DeleteWriteBatch(batch1)
// 	defer DeleteWriteBatch(batch2)

// 	batch1.PutCF(cfh1, keys[0], vals[0])
// 	batch1.PutCF(cfh2, keys[0], vals2[0])
// 	batch2.PutCF(cfh1, keys[1], vals[1])
// 	batch2.PutCF(cfh2, keys[1], vals2[1])
// 	batch1.PutCF(cfh1, keys[2], vals[2])
// 	batch2.PutCF(cfh2, keys[2], vals2[2])

// 	kvdb.Write(batch1)
// 	kvdb.Write(batch2)

// 	it, _ := GetIterator(cfh1)
// 	it.SeekToFirst()
// 	for idx := 0; it.Valid(); it.Next() {
// 		if it.Value() != vals[idx] {
// 			t.Errorf("Expected: %v, get: %v\n", vals[idx], it.Value())
// 		}
// 		idx++
// 	}
// 	DeleteIterator(it)

// 	it, _ = GetIterator(cfh2)
// 	it.SeekToFirst()
// 	for idx := 0; it.Valid(); it.Next() {
// 		if it.Value() != vals2[idx] {
// 			t.Errorf("Expected: %v, get: %v\n", vals2[idx], it.Value())
// 		}
// 		idx++
// 	}
// 	DeleteIterator(it)
// }

// func check_result(res ustore.PairStatusString, t *testing.T) string {
// 	if !res.GetFirst().Ok() {
// 		t.Errorf("Error status from Result")
// 	}
// 	return res.GetSecond()
// }

// func correct(val, expect string, t *testing.T) {
// 	if val != expect {
// 		t.Errorf("Expected %v, got %v", expect, val)
// 	}
// }

// func TestKVDB_BlockOps(t *testing.T) {
// 	kvdb := ustore.NewKVDB(uint(42))
// 	keys := []string{"key1", "key2", "key3"}
// 	vals := []string{"val1", "val2", "val3"}

// 	res1 := check_result(kvdb.PutBlock(keys[0], vals[0]), t)

// 	// then put again
// 	res2 := check_result(kvdb.PutBlock(keys[0], vals[2]), t)

// 	// get latest map
// 	correct(check_result(kvdb.GetBlock(keys[0], res2), t), vals[2], t)
// 	// get previous version
// 	correct(check_result(kvdb.GetBlock(keys[0], res1), t), vals[0], t)
// 	//  fmt.Printf("version: %v, len %v\n", res1, len(res1))
// }

// func TestKVDB_BatchOps(t *testing.T) {
// 	kvdb := ustore.NewKVDB(uint(43))
// 	batch := ustore.NewWriteBatch()
// 	batch.Put("key1", "val1")
// 	batch.Put("key2", "val2")
// 	batch.Put("key3", "val3")
// 	st := kvdb.Write(batch)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}

// 	// read back value
// 	ret := kvdb.Get("key1")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str := "val1"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	ret = kvdb.Get("key2")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str = "val2"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	ret = kvdb.Get("key3")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str = "val3"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	// update values
// 	batch.Clear()
// 	batch.Put("key2", "222val222")
// 	batch.Put("key2", "val222")
// 	batch.Put("key3", "modified val3")
// 	st = kvdb.Write(batch)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}

// 	// check updated values
// 	ret = kvdb.Get("key2")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str = "val222"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	ret = kvdb.Get("key3")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str = "modified val3"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	// mixed delete and updates
// 	batch.Clear()
// 	batch.Delete("key1")
// 	batch.Put("key2", "update val2")
// 	batch.Delete("key3")
// 	batch.Put("key4", "val4")
// 	st = kvdb.Write(batch)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}

// 	// check deleted and updated values
// 	ret = kvdb.Get("key1")
// 	if ret.GetFirst().Ok() {
// 		t.Errorf("Unexpected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if !ret.GetFirst().IsNotFound() {
// 		t.Errorf("Expected not_found status, but get: '%s'", ret.GetFirst().ToString())
// 	}

// 	ret = kvdb.Get("key2")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str = "update val2"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	ret = kvdb.Get("key3")
// 	if ret.GetFirst().Ok() {
// 		t.Errorf("Unexpected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	if !ret.GetFirst().IsNotFound() {
// 		t.Errorf("Expected not_found status, but get: '%s'", ret.GetFirst().ToString())
// 	}

// 	ret = kvdb.Get("key4")
// 	if !ret.GetFirst().Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", ret.GetFirst().ToString())
// 	}
// 	expected_str = "val4"
// 	if strings.Compare(ret.GetSecond(), expected_str) != 0 {
// 		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ret.GetSecond())
// 	}

// 	ustore.DeleteWriteBatch(batch)
// 	ustore.DeleteKVDB(kvdb)
// }

// func TestKVDB_Iterator(t *testing.T) {
// 	kvdb := ustore.NewKVDB(uint(44))
// 	batch := ustore.NewWriteBatch()
// 	// insert values
// 	batch.Put("key1", "val1")
// 	batch.Put("key2", "val2")
// 	batch.Put("key3", "val3")
// 	batch.Put("key4", "val4")
// 	batch.Put("key5", "val5")
// 	batch.Put("key6", "val6")
// 	batch.Put("key7", "val7")
// 	st := kvdb.Write(batch)
// 	if !st.Ok() {
// 		t.Errorf("Expected ok status, but get: '%s'", st.ToString())
// 	}

// 	// new Iterator
// 	it := kvdb.NewIterator()
// 	if it.Valid() {
// 		t.Errorf("Unexpected valid iterator status")
// 	}

// 	// test seek to first
// 	it.SeekToFirst()
// 	if !it.Valid() {
// 		t.Errorf("Unexpected invalid iterator status")
// 	}
// 	key := it.Key()
// 	expected_str := "key1"
// 	if strings.Compare(key, expected_str) != 0 {
// 		t.Errorf("Expected str is '%s', but get: '%s'", expected_str, key)
// 	}
// 	value := it.Value()
// 	expected_str = "val1"
// 	if strings.Compare(value, expected_str) != 0 {
// 		t.Errorf("Expected str is '%s', but get: '%s'", expected_str, value)
// 	}
// 	it.Prev()
// 	if it.Valid() {
// 		t.Errorf("Unexpected valid iterator status")
// 	}

// 	// test seek to last
// 	it.SeekToLast()
// 	if !it.Valid() {
// 		t.Errorf("Unexpected invalid iterator status")
// 	}
// 	key = it.Key()
// 	expected_str = "key7"
// 	if strings.Compare(key, expected_str) != 0 {
// 		t.Errorf("Expected str is '%s', but get: '%s'", expected_str, key)
// 	}
// 	value = it.Value()
// 	expected_str = "val7"
// 	if strings.Compare(value, expected_str) != 0 {
// 		t.Errorf("Expected str is '%s', but get: '%s'", expected_str, value)
// 	}
// 	it.Next()
// 	if it.Valid() {
// 		t.Errorf("Unexpected valid iterator status")
// 	}

// 	// test seek to existing key
// 	it.Seek("key3")
// 	if !it.Valid() {
// 		t.Errorf("Unexpected invalid iterator status")
// 	}
// 	key = it.Key()
// 	expected_str = "key3"
// 	if strings.Compare(key, expected_str) != 0 {
// 		t.Errorf("Expected str is '%s', but get: '%s'", expected_str, key)
// 	}
// 	value = it.Value()
// 	expected_str = "val3"
// 	if strings.Compare(value, expected_str) != 0 {
// 		t.Errorf("Expected str is '%s', but get: '%s'", expected_str, value)
// 	}

// 	ustore.DeleteIterator(it)
// 	ustore.DeleteKVDB(kvdb)
// }
