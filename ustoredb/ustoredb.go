// DB wrapper for USTORE
package ustoredb

import (
	"fmt"
	"github.com/hyperledger/fabric/ustore"
	"github.com/op/go-logging"
	"sync"
)

// column family namespace
type ColumnFamilyHandle struct {
	db   ustore.KVDB // pointer to DB partition
	name string
}

var ulogger = logging.MustGetLogger("ustoreDB")

// wrap write batch, indexed by ColumnFamily name
type WriteBatch struct {
	updates map[string]ustore.WriteBatch
}

type UStoreDB struct {
	db        ustore.KVDB // default DB partition
	cFamilies map[string]*ColumnFamilyHandle
	ncfs      uint32 // number of column families
}

var once sync.Once

func OpenDB() (*UStoreDB, error) {
	db := ustore.NewKVDB(uint(0))
	return &UStoreDB{db, make(map[string]*ColumnFamilyHandle), 1}, nil
}

func Close(db *UStoreDB) {
	ustore.DeleteKVDB(db.db)

	/*
	  for cf := range db.cFamilies {
	    delete(db.cFamilies, cf)
	  }*/
}

func NewWriteBatch() (*WriteBatch, error) {
	return &WriteBatch{make(map[string]ustore.WriteBatch)}, nil
}

func DeleteWriteBatch(batch *WriteBatch) {
	for _, b := range batch.updates {
		ustore.DeleteWriteBatch(b)
	}
}

func GetIterator(cfh *ColumnFamilyHandle) (ustore.Iterator, error) {
	return cfh.db.NewIterator(), nil
}

func (cfh *ColumnFamilyHandle) GetCFName() string {
	return cfh.name
}

func DeleteIterator(it ustore.Iterator) {
	ustore.DeleteIterator(it)
}

func (writebatch *WriteBatch) DeleteCF(cfh *ColumnFamilyHandle, key string) {
	if wb, ok := writebatch.updates[cfh.name]; ok {
		wb.Delete(key)
	}
}

func (writebatch *WriteBatch) Clear() {
	for _, wb := range writebatch.updates {
		wb.Clear()
	}
}

func (writebatch *WriteBatch) PutCF(cfh *ColumnFamilyHandle, key string, value string) error {
	// gathering updates
	if wb, ok := writebatch.updates[cfh.name]; ok {
		// CF existed
		wb.Put(key, value)
	} else {
		tmp := ustore.NewWriteBatch()
		tmp.Put(key, value)
		writebatch.updates[cfh.name] = tmp
	}
	return nil
}

func (db *UStoreDB) GetDB() ustore.KVDB {
	return db.db
}

func (db *UStoreDB) GetSize() uint64 {
	return uint64(db.db.GetSize())
}
func (db *UStoreDB) InitGlobalState() error {
	if !db.db.InitGlobalState().Ok() {
		panic("Failed to init global state")
	}
	return nil
}

func (db *UStoreDB) GetState(key []byte) (string, error) {
	var res ustore.PairStatusString
	res = db.db.GetState(string(key))
	if !res.GetFirst().Ok() {
		return "", fmt.Errorf("Failed to get state for %v", key)
	} else {
		return res.GetSecond(), nil
	}
	return "", nil
}

func (db *UStoreDB) PutState(key, value []byte, txnID string, deps [][]byte) error {
	dep_vec := ustore.NewVecStr()
	ulogger.Infof("Key = %s, value = %s txnID = %s", string(key), string(value), txnID)
	for _, dep := range deps {
		ulogger.Infof("dep: %s", string(dep))
		dep_vec.Add(string(dep))
	}
	if !db.db.PutState(string(key), string(value), txnID, dep_vec) {
		panic("Fail to put state")
	}
	return nil
}

func (db *UStoreDB) GetBlock(key, version []byte) (string, error) {
	var res ustore.PairStatusString
	res = db.db.GetBlock(string(key), string(version))
	if !res.GetFirst().Ok() {
		return "", fmt.Errorf("Failed to get map")
	} else {
		return res.GetSecond(), nil
	}
}

func (db *UStoreDB) PutBlock(key, value []byte) (string, error) {
	if res := db.db.PutBlock(string(key[:]), string(value[:])); res.GetFirst().Ok() {
		return res.GetSecond(), nil
	} else {
		panic("Failed to Put Blob")
	}
	return "", nil
}

func (db *UStoreDB) Commit() (string, error) {
	if res := db.db.Commit(); res.GetFirst().Ok() {
		return res.GetSecond(), nil
	} else {
		panic("Failed to Commit Global State")
	}
	return "", nil
}

func (db *UStoreDB) CreateColumnFamily(cfname string) (*ColumnFamilyHandle, error) {
	if _, ok := db.cFamilies[cfname]; ok {
		return nil, fmt.Errorf("Column family %v already existed", cfname)
	} else {
		cfh := &ColumnFamilyHandle{ustore.NewKVDB(uint(db.ncfs), cfname), cfname}
		db.ncfs++
		db.cFamilies[cfname] = cfh
		return cfh, nil
	}
}

func (db *UStoreDB) DropColumnFamily(cfh *ColumnFamilyHandle) error {
	delete(db.cFamilies, cfh.name)
	return nil
}

func DeleteColumnFamilyHandle(cfh *ColumnFamilyHandle) {
	ustore.DeleteKVDB(cfh.db)
}

func (db *UStoreDB) PutCF(cfh *ColumnFamilyHandle, key string, value string) error {
	if err := cfh.db.Put(key, value); err.Ok() {
		return nil
	} else {
		return fmt.Errorf("Error during Put")
	}
}

func (db *UStoreDB) Write(writebatch *WriteBatch) error {
	for k, v := range writebatch.updates {
		db.cFamilies[k].db.Write(v)
	}
	return nil
}

func (db *UStoreDB) DeleteCF(cfh *ColumnFamilyHandle, key string) error {
	if err := cfh.db.Delete(key); err.Ok() {
		return nil
	} else {
		return fmt.Errorf("Error during Delete")
	}
}

func (db *UStoreDB) ExistCF(cfh *ColumnFamilyHandle, key string) error {
	if err := cfh.db.Exist(key); err {
		return nil
	} else {
		return fmt.Errorf("Error during Exist")
	}
}

func (db *UStoreDB) GetCF(cfh *ColumnFamilyHandle, key string) (string, error) {
	if err := cfh.db.Get(key); err.GetFirst().Ok() {
		return err.GetSecond(), nil
	} else {
		return "", fmt.Errorf("Error during Get")
	}
}
