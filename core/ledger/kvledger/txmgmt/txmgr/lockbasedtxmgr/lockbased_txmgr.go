/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package lockbasedtxmgr

import (
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/statebasedval"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/protos/common"
  "github.com/hyperledger/fabric/core/chaincode/shim"

  pc "provchain"
  "errors"
  "strings"
  "encoding/json"
)

var logger = flogging.MustGetLogger("lockbasedtxmgr")

// LockBasedTxMgr a simple implementation of interface `txmgmt.TxMgr`.
// This implementation uses a read-write lock to prevent conflicts between transaction simulation and committing
type LockBasedTxMgr struct {
	db           statedb.VersionedDB
	validator    validator.Validator
	provchain    pc.ProvChain
	batch        *statedb.UpdateBatch
	currentBlock *common.Block
	commitRWLock sync.RWMutex
}

// NewLockBasedTxMgr constructs a new instance of NewLockBasedTxMgr
func NewLockBasedTxMgr(db statedb.VersionedDB) *LockBasedTxMgr {
	db.Open()
	return &LockBasedTxMgr{db: db, validator: statebasedval.NewValidator(db), provchain: pc.NewProvChain()}
}

// GetLastSavepoint returns the block num recorded in savepoint,
// returns 0 if NO savepoint is found
func (txmgr *LockBasedTxMgr) GetLastSavepoint() (*version.Height, error) {
	return txmgr.db.GetLatestSavePoint()
}

// NewQueryExecutor implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) NewQueryExecutor() (ledger.QueryExecutor, error) {
	qe := newQueryExecutor(txmgr)
	txmgr.commitRWLock.RLock()
	return qe, nil
}

// NewTxSimulator implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) NewTxSimulator() (ledger.TxSimulator, error) {
	logger.Debugf("constructing new tx simulator")
	s := newLockBasedTxSimulator(txmgr)
	txmgr.commitRWLock.RLock()
	return s, nil
}

// ValidateAndPrepare implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) ValidateAndPrepare(block *common.Block, doMVCCValidation bool) error {
	logger.Debugf("Validating new block with num trans = [%d]", len(block.Data.Data))
	batch, err := txmgr.validator.ValidateAndPrepareBatch(block, doMVCCValidation)
	if err != nil {
		return err
	}
	txmgr.currentBlock = block
	txmgr.batch = batch
	return err
}

// Shutdown implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) Shutdown() {
	txmgr.db.Close()
}

// Commit implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) Commit() error {
	logger.Debugf("Committing updates to state database")
	txmgr.commitRWLock.Lock()
	defer txmgr.commitRWLock.Unlock()
	logger.Debugf("Write lock acquired for committing updates to state database")
	if txmgr.batch == nil {
		panic("validateAndPrepare() method should have been called before calling commit()")
	}
	defer func() { txmgr.batch = nil }()
	if err := txmgr.db.ApplyUpdates(txmgr.batch,
		version.NewHeight(txmgr.currentBlock.Header.Number, uint64(len(txmgr.currentBlock.Data.Data)-1))); err != nil {
		return err
	}
	logger.Debugf("Updates committed to state database")

	if err := txmgr.CommitProvchain(txmgr.batch); err != nil {
		return err
	}

	logger.Debugf("Updates committed to provchain")
	return nil
}

func (txmgr *LockBasedTxMgr) CommitProvchain(batch *statedb.UpdateBatch) error {
	namespaces := batch.GetUpdatedNamespaces()
	for _, ns := range namespaces {
		updates := batch.GetUpdates(ns)
		for k, vv := range updates {
		  if strings.HasSuffix(k, "_prov") {
		  	orig_key := k[0: len(k)-5]
		  	logger.Debugf("Handle Provenance for Key %s for ns %s.", 
		  		orig_key, ns)

		    compositeStrKey := ns + "_" + orig_key
			  blk_idx :=  vv.Version.BlockNum

			  prov_meta := &shim.ProvenanceMeta{}
			  logger.Debugf("Unmarshal provenance record: %s", string(vv.Value))
			  err := json.Unmarshal([]byte(vv.Value), prov_meta) 
			  if err != nil {
			    logger.Warningf("Error at unmarshalling provenance record %s", 
			    	          string(vv.Value))
			    return err
			  }
			  logger.Debugf("CompositeStrKey = %s. ", compositeStrKey)
			  logger.Debugf("Txn = %s. ", prov_meta.TxID)
			  logger.Debugf("Val = %s", prov_meta.Val)
			  logger.Debugf("Blk_Idx = %s", blk_idx)

			  dep_reads := pc.NewStringVector()
			  for i := 0; i < len(prov_meta.DepReads); i++ {
			    var dep_read string; 
			    dep_read = ns + "_" + prov_meta.DepReads[i]
			    dep_reads.Add(dep_read)
			    logger.Debugf("DepReads[%d] = %s", i, dep_read) 
			  }

				if !txmgr.provchain.PutState(compositeStrKey, blk_idx, prov_meta.Val, false) { 
					return errors.New("Fail to put state on provchain")
				}

			  if !txmgr.provchain.LinkState(compositeStrKey, blk_idx, dep_reads, prov_meta.TxID) {
					return errors.New("Fail to Link state on provchain")
			  }
		  } 
	  }
	}
	return nil
}

// Rollback implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) Rollback() {
	txmgr.batch = nil
}

// ShouldRecover implements method in interface kvledger.Recoverer
func (txmgr *LockBasedTxMgr) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	savepoint, err := txmgr.GetLastSavepoint()
	if err != nil {
		return false, 0, err
	}
	if savepoint == nil {
		return true, 0, nil
	}
	return savepoint.BlockNum != lastAvailableBlock, savepoint.BlockNum + 1, nil
}

// CommitLostBlock implements method in interface kvledger.Recoverer
func (txmgr *LockBasedTxMgr) CommitLostBlock(block *common.Block) error {
	logger.Debugf("Constructing updateSet for the block %d", block.Header.Number)
	if err := txmgr.ValidateAndPrepare(block, false); err != nil {
		return err
	}
	logger.Debugf("Committing block %d to state database", block.Header.Number)
	if err := txmgr.Commit(); err != nil {
		return err
	}
	return nil
}
