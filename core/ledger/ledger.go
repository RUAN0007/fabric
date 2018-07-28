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

package ledger

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/anh/tecbot/gorocksdb"
	"github.com/hyperledger/fabric/core/db"
	"github.com/hyperledger/fabric/core/ledger/statemgmt"
	"github.com/hyperledger/fabric/core/ledger/statemgmt/state"
	"github.com/hyperledger/fabric/core/util"
	"github.com/hyperledger/fabric/events/producer"
	"github.com/op/go-logging"

	"github.com/hyperledger/fabric/protos"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

var ledgerLogger = logging.MustGetLogger("ledger")

//ErrorType represents the type of a ledger error
type ErrorType string

const (
	//ErrorTypeInvalidArgument used to indicate the invalid input to ledger method
	ErrorTypeInvalidArgument = ErrorType("InvalidArgument")
	//ErrorTypeOutOfBounds used to indicate that a request is out of bounds
	ErrorTypeOutOfBounds = ErrorType("OutOfBounds")
	//ErrorTypeResourceNotFound used to indicate if a resource is not found
	ErrorTypeResourceNotFound = ErrorType("ResourceNotFound")
	//ErrorTypeBlockNotFound used to indicate if a block is not found when looked up by it's hash
	ErrorTypeBlockNotFound = ErrorType("ErrorTypeBlockNotFound")
)

//Error can be used for throwing an error from ledger code.
type Error struct {
	errType ErrorType
	msg     string
}

func (ledgerError *Error) Error() string {
	return fmt.Sprintf(" LedgerError - %s: %s", ledgerError.errType, ledgerError.msg)
}

//Type returns the type of the error
func (ledgerError *Error) Type() ErrorType {
	return ledgerError.errType
}

func newLedgerError(errType ErrorType, msg string) *Error {
	return &Error{errType, msg}
}

var (
	// ErrOutOfBounds is returned if a request is out of bounds
	ErrOutOfBounds = newLedgerError(ErrorTypeOutOfBounds, "ledger: out of bounds")

	// ErrResourceNotFound is returned if a resource is not found
	ErrResourceNotFound = newLedgerError(ErrorTypeResourceNotFound, "ledger: resource not found")
)

// Ledger - the struct for openchain ledger
type Ledger struct {
	blockchain     *blockchain
	state          *state.State
	currentID      interface{}
	statUtil       *util.StatUtil
	nReads         uint64 // number of read
	nWrites        uint64 // number of write
	totalReadTime  uint64 // read time
	totalWriteTime uint64 // write time
}

var ledger *Ledger
var ledgerError error
var once sync.Once

// GetLedger - gives a reference to a 'singleton' ledger
func GetLedger() (*Ledger, error) {
	once.Do(func() {
		ledger, ledgerError = GetNewLedger()
		config := viper.New()
		config.SetEnvPrefix("ledger") // variable is LEDGER_SAMPLE_INTERNVAL
		config.AutomaticEnv()
		sampleInterval := config.GetInt("sample_interval")
		ledgerLogger.Infof("Sample interval : %v", sampleInterval)
		ledger.statUtil.NewStat("ledgerput", uint32(sampleInterval))
		ledger.statUtil.NewStat("ledgerget", uint32(sampleInterval))
		ledger.statUtil.NewStat("txn", uint32(sampleInterval))
        ledger.statUtil.NewStat("block", uint32(sampleInterval))
		ledger.nReads = 0
		ledger.nWrites = 0
		ledger.totalReadTime = 0
		ledger.totalWriteTime = 0
		db.GetDBHandle().DB.InitGlobalState()
	})
	return ledger, ledgerError
}

// GetNewLedger - gives a reference to a new ledger TODO need better approach
func GetNewLedger() (*Ledger, error) {
	blockchain, err := newBlockchain()
	if err != nil {
		return nil, err
	}

	state := state.NewState()
	return &Ledger{blockchain, state, nil, util.GetStatUtil(), 0, 0, 0, 0}, nil
}

// Stat collection, querying via OpenChain REST API
// Return (#reads, #writes, read time, write time)
func (ledger *Ledger) GetDBStats() (uint64, uint64, uint64, uint64, uint64) {
	dbsize := db.GetDBHandle().DB.GetSize()
	return ledger.nReads, ledger.nWrites, ledger.totalReadTime, ledger.totalWriteTime, dbsize
}

/////////////////// Transaction-batch related methods ///////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

// BeginTxBatch - gets invoked when next round of transaction-batch execution begins
func (ledger *Ledger) BeginTxBatch(id interface{}) error {
	err := ledger.checkValidIDBegin()
	if err != nil {
		return err
	}
	ledger.currentID = id
	return nil
}

// GetTXBatchPreviewBlockInfo returns a preview block info that will
// contain the same information as GetBlockchainInfo will return after
// ledger.CommitTxBatch is called with the same parameters. If the
// state is modified by a transaction between these two calls, the
// contained hash will be different.
func (ledger *Ledger) GetTXBatchPreviewBlockInfo(id interface{},
	transactions []*protos.Transaction, metadata []byte) (*protos.BlockchainInfo, error) {
	panic("Not implemented...")
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return nil, err
	}
	stateHash, err := ledger.state.GetHash()
	if err != nil {
		return nil, err
	}
	block := ledger.blockchain.buildBlock(protos.NewBlock(transactions, metadata), stateHash)
	info := ledger.blockchain.getBlockchainInfoForBlock(ledger.blockchain.getSize()+1, block)
	return info, nil
}


func MeasureBFSLevel(ccid, key string, blk_idx uint64, level uint64) {
	long_key := string(statemgmt.ConstructCompositeKey(ccid, key))
	dep_keys, dep_versions, err := db.GetDBHandle().DB.GetDeps(long_key, blk_idx)
	if err != nil {
		panic("Fail to get dependency for " + long_key + " at blk idx " + strconv.Itoa(int(blk_idx)))
	}
    MEASURE_TIMES := 1
    total_duration := 0
	
    for j := 0; j < MEASURE_TIMES;j++ {
	    startTime := time.Now()
    	for i := 1; i < int(level); i++ {
    		// ledgerLogger.Infof("Level %d:  # of Deps Keys = %d and versions = %d", i, len(dep_keys), len(dep_versions))
    		var next_dep_keys []string
    		var next_dep_versions []string
    
    		for ii, dep_key := range dep_keys {
    			dep_version := dep_versions[ii]
    			_, state_err := db.GetDBHandle().DB.GetHistoricalStateVersion(dep_key, dep_version)
    			if state_err == nil {
    				cur_dep_keys, cur_dep_versions, prov_err := db.GetDBHandle().DB.GetDepsVersion(dep_key, dep_version)
    				if prov_err == nil {
    					next_dep_keys = append(next_dep_keys, cur_dep_keys...)
    					next_dep_versions = append(next_dep_versions, cur_dep_versions...)
    				} } } // end for
    
    		dep_keys = make([]string, len(next_dep_keys))
    		copy(dep_keys, next_dep_keys)
    
    		dep_versions = make([]string, len(next_dep_versions))
    		copy(dep_versions, next_dep_versions)
    	} // end for level
  	    duration := uint64(time.Since(startTime))
        total_duration = total_duration + int(duration)
    }
	long_key = long_key + " " + strconv.Itoa(int(blk_idx))
	ledgerLogger.Infof("BFS Query with Level %d Duration for  %s is %d", level, long_key, total_duration / MEASURE_TIMES) 
  }




func UnionFind_DFS(ccid string, max_level uint64, 
         key1 string, blk_idx1 uint64,
         key2 string, blk_idx2 uint64) {
  startTime := time.Now()
  wqu := util.NewWeightedQuickUnion()

  long_key1 := string(statemgmt.ConstructCompositeKey(ccid, key1))
  key_stack1, version_stack1, err := db.GetDBHandle().DB.GetDeps(long_key1, blk_idx1)
  if err != nil {
	panic("Fail to get dependency for " + long_key1 + " at blk idx " + strconv.Itoa(int(blk_idx1)))
  }
  level_stack1 := make([]uint64,len(key_stack1)) 
  
  for i := 0; i < len(level_stack1); i=i+1 {
    level_stack1[i] = 1
    key := key_stack1[i]
    version := version_stack1[i]
    node := key + "_" + version
    wqu.Union(long_key1, node)
  }


  long_key2 := string(statemgmt.ConstructCompositeKey(ccid, key2))
  key_stack2, version_stack2, err := db.GetDBHandle().DB.GetDeps(long_key2, blk_idx2)
  if err != nil {
	panic("Fail to get dependency for " + long_key2 + " at blk idx " + strconv.Itoa(int(blk_idx2)))
  }
  level_stack2 := make([]uint64,len(key_stack2)) 
  for i := 0; i < len(level_stack2); i=i+1 {
    level_stack2[i] = 1
    key := key_stack2[i]
    version := version_stack2[i]
    node := key + "_" + version
    wqu.Union(long_key2, node)
  }

  var last_key1 string
  var last_version1 string
  var last_level1 uint64 

  var last_key2 string
  var last_version2 string
  var last_level2 uint64 

  for len(key_stack1) > 0 || len(key_stack2) > 0 {
    if l1 := len(key_stack1); l1 > 0 {
      last_key1, key_stack1 = key_stack1[l1-1], key_stack1[:l1-1]
      last_version1, version_stack1 = version_stack1[l1-1], version_stack1[:l1-1]
      last_level1, level_stack1 = level_stack1[l1-1], level_stack1[:l1-1]
      main_node := last_key1 + "_" + last_version1
      if (last_level1 < uint64(max_level)) {
        cur_dep_keys1, cur_dep_versions1, prov_err := db.GetDBHandle().DB.GetDepsVersion(last_key1, last_version1)
        if prov_err != nil {
          panic("Fail to get provenance1 for " + last_key1 + " with version " + last_version1)
        }
        key_stack1 = append(key_stack1, cur_dep_keys1...)
        version_stack1 = append(version_stack1, cur_dep_versions1...)
        for i := 0; i < len(cur_dep_keys1); i=i+1 {
          level_stack1 = append(level_stack1, last_level1 + 1)
          node := key_stack1[i] + "_" + version_stack1[i]
          wqu.Union(main_node, node) 
        }  // end for i
      }  // end if
    }  // end if l1 


    if l2 := len(key_stack2); l2 > 0 {
      last_key2, key_stack2 = key_stack2[l2-1], key_stack2[:l2-1]
      last_version2, version_stack2 = version_stack2[l2-1], version_stack2[:l2-1]
      last_level2, level_stack2 = level_stack2[l2-1], level_stack2[:l2-1]
      main_node := last_key2 + "_" + last_version2
      
      if (last_level2 < uint64(max_level)) {
        cur_dep_keys2, cur_dep_versions2, prov_err := db.GetDBHandle().DB.GetDepsVersion(last_key2, last_version2)
        if prov_err != nil {
          panic("Fail to get provenance2 for " + last_key2 + " with version " + last_version2)
        }
        key_stack2 = append(key_stack2, cur_dep_keys2...)
        version_stack2 = append(version_stack2, cur_dep_versions2...)
        for i := 0; i < len(cur_dep_keys2); i=i+1 {
          level_stack2 = append(level_stack2, last_level2 + 1)
          node := key_stack2[i] + "_" + version_stack2[i]
          wqu.Union(main_node, node) 
        }  // end for i
      }  // end if
    }  // end if l2 
  }  // end for
  
  connected := wqu.Connected(long_key1, long_key2) 
  duration := uint64(time.Since(startTime))
  ledgerLogger.Infof("Union Find (%s, %s) = %d, with Max Level %d and Duration %d", long_key1, long_key2, connected, max_level, duration)
}


// CommitTxBatch - gets invoked when the current transaction-batch needs to be committed
// This function returns successfully iff the transactions details and state changes (that
// may have happened during execution of this transaction-batch) have been committed to permanent storage
func (ledger *Ledger) CommitTxBatch(id interface{}, transactions []*protos.Transaction, transactionResults []*protos.TransactionResult, metadata []byte) error {
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return err
	}

	//stateHash, err := ledger.state.GetHash()
	ledger.state.PrepareToCommit()
	stateHash, err := ledger.state.GetUStoreHash()
	if err != nil {
		ledger.resetForNextTxGroup(false)
		ledger.blockchain.blockPersistenceStatus(false)
		return err
	}

	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	block := protos.NewBlock(transactions, metadata)
	ccEvents := []*protos.ChaincodeEvent{}

	if transactionResults != nil {
		ccEvents = make([]*protos.ChaincodeEvent, len(transactionResults))
		for i := 0; i < len(transactionResults); i++ {
			if transactionResults[i].ChaincodeEvent != nil {
				ccEvents[i] = transactionResults[i].ChaincodeEvent
			} else {
				//We need the index so we can map the chaincode
				//event to the transaction that generated it.
				//Hence need an entry for cc event even if one
				//wasn't generated for the transaction. We cannot
				//use a nil cc event as protobuf does not like
				//elements of a repeated array to be nil.
				//
				//We should discard empty events without chaincode
				//ID when sending out events.
				ccEvents[i] = &protos.ChaincodeEvent{}
			}
		}
	}

	//store chaincode events directly in NonHashData. This will likely change in New Consensus where we can move them to Transaction
	block.NonHashData = &protos.NonHashData{ChaincodeEvents: ccEvents}
	newBlockNumber, err := ledger.blockchain.addPersistenceChangesForNewBlock(context.TODO(), block, stateHash, writeBatch)
    if lt, ok := ledger.statUtil.Stats["block"].End(strconv.FormatUint(newBlockNumber, 10)); ok {
      ledgerLogger.Infof("Block Interval: %v", lt)
    }
    ledger.statUtil.Stats["block"].Start(strconv.FormatUint(newBlockNumber+1, 10))

	if err != nil {
		panic("Something wrong during commit...")
		ledger.resetForNextTxGroup(false)
		ledger.blockchain.blockPersistenceStatus(false)
		return err
	}

	//ledger.state.AddChangesForPersistence(newBlockNumber, writeBatch)
	opt := gorocksdb.NewDefaultWriteOptions()
	defer opt.Destroy()
	dbErr := db.GetDBHandle().DB.Write(opt, writeBatch)
	if dbErr != nil {
		panic("Something wrong during commit ...")
		ledger.resetForNextTxGroup(false)
		ledger.blockchain.blockPersistenceStatus(false)
		return dbErr
	}

	ledger.resetForNextTxGroup(true)
	ledger.blockchain.blockPersistenceStatus(true)

	sendProducerBlockEvent(block)

	//send chaincode events from transaction results
	sendChaincodeEvents(transactionResults)

	if len(transactionResults) != 0 {
		ledgerLogger.Debugf("There were some erroneous transactions. We need to send a 'TX rejected' message here.")
	}
	ledgerLogger.Infof("Commited block %v, hash:%v", newBlockNumber, stateHash)

	if newBlockNumber == 160 {
      //UnionFind_DFS("supplychain", 1, "Phone_0", uint64(newBlockNumber), "Phone_1", uint64(newBlockNumber))
      MeasureBFSLevel("supplychain", "Phone_0", newBlockNumber, 1)
      for i := 1; i <= 6;i=i+1 {
        //UnionFind_DFS("supplychain", uint64(i), "Phone_0", uint64(newBlockNumber), "Phone_1", uint64(newBlockNumber))
        MeasureBFSLevel("supplychain", "Phone_0", newBlockNumber, uint64(i))
      }
      panic("Stop here")
	}

	return nil
}

// RollbackTxBatch - Discards all the state changes that may have taken place during the execution of
// current transaction-batch
func (ledger *Ledger) RollbackTxBatch(id interface{}) error {
	panic("Rollback not implemented")
	ledgerLogger.Debugf("RollbackTxBatch for id = [%s]", id)
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return err
	}
	ledger.resetForNextTxGroup(false)
	return nil
}

// TxBegin - Marks the begin of a new transaction in the ongoing batch
func (ledger *Ledger) TxBegin(txID string) {
	ledger.statUtil.Stats["txn"].Start(txID)
	ledger.state.TxBegin(txID)
}

// TxFinished - Marks the finish of the on-going transaction.
// If txSuccessful is false, the state changes made by the transaction are discarded
func (ledger *Ledger) TxFinished(txID string, txSuccessful bool) {
	ledger.state.TxFinish(txID, txSuccessful)
	if val, ok := ledger.statUtil.Stats["txn"].End(txID); ok {
		ledgerLogger.Infof("Txn Execution latency: %v", val)
	}
}

/////////////////// world-state related methods /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

// GetTempStateHash - Computes state hash by taking into account the state changes that may have taken
// place during the execution of current transaction-batch
func (ledger *Ledger) GetTempStateHash() ([]byte, error) {
	return ledger.state.GetUStoreHash()
	//return nil, nil
}

// GetTempStateHashWithTxDeltaStateHashes - In addition to the state hash (as defined in method GetTempStateHash),
// this method returns a map [txUuid of Tx --> cryptoHash(stateChangesMadeByTx)]
// Only successful txs appear in this map
func (ledger *Ledger) GetTempStateHashWithTxDeltaStateHashes() ([]byte, map[string][]byte, error) {
	panic("GetTempStateHashWithTxDeltaStateHashes not implemented")
	stateHash, err := ledger.state.GetHash()
	return stateHash, ledger.state.GetTxStateDeltaHash(), err
}

// GetState get state for chaincodeID and key. If committed is false, this first looks in memory
// and if missing, pulls from db.  If committed is true, this pulls from the db only.
func (ledger *Ledger) GetState(chaincodeID string, key string, committed bool) ([]byte, error) {
	ledger.statUtil.Stats["ledgerget"].Start(key)
	ledger.nReads++
	startTime := time.Now()
	res, err := ledger.state.Get(chaincodeID, key, committed)
	ledger.totalReadTime += uint64(time.Since(startTime))
	if val, ok := ledger.statUtil.Stats["ledgerget"].End(key); ok {
		ledgerLogger.Infof("GetState latency: %v", val)
	}
	return res, err
}

// GetStateRangeScanIterator returns an iterator to get all the keys (and values) between startKey and endKey
// (assuming lexical order of the keys) for a chaincodeID.
// If committed is true, the key-values are retrieved only from the db. If committed is false, the results from db
// are mergerd with the results in memory (giving preference to in-memory data)
// The key-values in the returned iterator are not guaranteed to be in any specific order
func (ledger *Ledger) GetStateRangeScanIterator(chaincodeID string, startKey string, endKey string, committed bool) (statemgmt.RangeScanIterator, error) {
	panic("GetStateRangeScane not implemented")
	return ledger.state.GetRangeScanIterator(chaincodeID, startKey, endKey, committed)
}

// SetState sets state to given value for chaincodeID and key. Does not immideatly writes to DB
func (ledger *Ledger) SetState(chaincodeID string, key string, value []byte, deps []string) error {
	if key == "" || value == nil {
		return newLedgerError(ErrorTypeInvalidArgument,
			fmt.Sprintf("An empty string key or a nil value is not supported. Method invoked with key='%s', value='%#v'", key, value))
	}
	ledgerLogger.Infof("Put ccid %s Key %s for Val %s and Deps %v", chaincodeID, key, string(value), deps)
	ledger.statUtil.Stats["ledgerput"].Start(key)
	ledger.nWrites++
	startTime := time.Now()
	res := ledger.state.Set(chaincodeID, key, value, deps)
	ledger.totalWriteTime += uint64(time.Since(startTime))
	if val, ok := ledger.statUtil.Stats["ledgerput"].End(key); ok {
		ledgerLogger.Infof("PutState latency: %v", val)
	}
	return res
}

// DeleteState tracks the deletion of state for chaincodeID and key. Does not immediately writes to DB
func (ledger *Ledger) DeleteState(chaincodeID string, key string) error {
	return ledger.state.Delete(chaincodeID, key)
}

// CopyState copies all the key-values from sourceChaincodeID to destChaincodeID
func (ledger *Ledger) CopyState(sourceChaincodeID string, destChaincodeID string) error {
	panic("CopyState not impelemnted")
	return ledger.state.CopyState(sourceChaincodeID, destChaincodeID)
}

// GetStateMultipleKeys returns the values for the multiple keys.
// This method is mainly to amortize the cost of grpc communication between chaincode shim peer
func (ledger *Ledger) GetStateMultipleKeys(chaincodeID string, keys []string, committed bool) ([][]byte, error) {
	panic("GetStateMultipleKey not implemented!")
	return ledger.state.GetMultipleKeys(chaincodeID, keys, committed)
}

// SetStateMultipleKeys sets the values for the multiple keys.
// This method is mainly to amortize the cost of grpc communication between chaincode shim peer
func (ledger *Ledger) SetStateMultipleKeys(chaincodeID string, kvs map[string][]byte) error {
	panic("SetStateMultipleKeys not implemented!")
	return ledger.state.SetMultipleKeys(chaincodeID, kvs)
}

// GetStateSnapshot returns a point-in-time view of the global state for the current block. This
// should be used when transferring the state from one peer to another peer. You must call
// stateSnapshot.Release() once you are done with the snapshot to free up resources.
func (ledger *Ledger) GetStateSnapshot() (*state.StateSnapshot, error) {
	panic("GetStateSnapshot not implemented")
	dbSnapshot := db.GetDBHandle().GetSnapshot()
	blockHeight, err := fetchBlockchainSizeFromSnapshot(dbSnapshot)
	if err != nil {
		dbSnapshot.Release()
		return nil, err
	}
	if 0 == blockHeight {
		dbSnapshot.Release()
		return nil, fmt.Errorf("Blockchain has no blocks, cannot determine block number")
	}
	return ledger.state.GetSnapshot(blockHeight-1, dbSnapshot)
}

// GetStateDelta will return the state delta for the specified block if
// available.  If not available because it has been discarded, returns nil,nil.
func (ledger *Ledger) GetStateDelta(blockNumber uint64) (*statemgmt.StateDelta, error) {
	panic("GetStateDelta not implemented")
	if blockNumber >= ledger.GetBlockchainSize() {
		return nil, ErrOutOfBounds
	}
	return ledger.state.FetchStateDeltaFromDB(blockNumber)
}

// ApplyStateDelta applies a state delta to the current state. This is an
// in memory change only. You must call ledger.CommitStateDelta to persist
// the change to the DB.
// This should only be used as part of state synchronization. State deltas
// can be retrieved from another peer though the Ledger.GetStateDelta function
// or by creating state deltas with keys retrieved from
// Ledger.GetStateSnapshot(). For an example, see TestSetRawState in
// ledger_test.go
// Note that there is no order checking in this function and it is up to
// the caller to ensure that deltas are applied in the correct order.
// For example, if you are currently at block 8 and call this function
// with a delta retrieved from Ledger.GetStateDelta(10), you would now
// be in a bad state because you did not apply the delta for block 9.
// It's possible to roll the state forwards or backwards using
// stateDelta.RollBackwards. By default, a delta retrieved for block 3 can
// be used to roll forwards from state at block 2 to state at block 3. If
// stateDelta.RollBackwards=false, the delta retrieved for block 3 can be
// used to roll backwards from the state at block 3 to the state at block 2.
func (ledger *Ledger) ApplyStateDelta(id interface{}, delta *statemgmt.StateDelta) error {
	panic("ApplyStateDetla not implemented")
	err := ledger.checkValidIDBegin()
	if err != nil {
		return err
	}
	ledger.currentID = id
	ledger.state.ApplyStateDelta(delta)
	return nil
}

// CommitStateDelta will commit the state delta passed to ledger.ApplyStateDelta
// to the DB
func (ledger *Ledger) CommitStateDelta(id interface{}) error {
	panic("CommitStateDelta not implemented")
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return err
	}
	defer ledger.resetForNextTxGroup(true)
	return ledger.state.CommitStateDelta()
}

// RollbackStateDelta will discard the state delta passed
// to ledger.ApplyStateDelta
func (ledger *Ledger) RollbackStateDelta(id interface{}) error {
	panic("RollbackStateDelta not implemented")
	err := ledger.checkValidIDCommitORRollback(id)
	if err != nil {
		return err
	}
	ledger.resetForNextTxGroup(false)
	return nil
}

// DeleteALLStateKeysAndValues deletes all keys and values from the state.
// This is generally only used during state synchronization when creating a
// new state from a snapshot.
func (ledger *Ledger) DeleteALLStateKeysAndValues() error {
	return ledger.state.DeleteState()
}

/////////////////// blockchain related methods /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

// GetBlockchainInfo returns information about the blockchain ledger such as
// height, current block hash, and previous block hash.
func (ledger *Ledger) GetBlockchainInfo() (*protos.BlockchainInfo, error) {
	return ledger.blockchain.getBlockchainInfo()
}

// GetBlockByNumber return block given the number of the block on blockchain.
// Lowest block on chain is block number zero
func (ledger *Ledger) GetBlockByNumber(blockNumber uint64) (*protos.Block, error) {
	if blockNumber >= ledger.GetBlockchainSize() {
		return nil, ErrOutOfBounds
	}
	return ledger.blockchain.getBlock(blockNumber)
}

// GetBlockchainSize returns number of blocks in blockchain
func (ledger *Ledger) GetBlockchainSize() uint64 {
	return ledger.blockchain.getSize()
}

// GetTransactionByID return transaction by it's txId
func (ledger *Ledger) GetTransactionByID(txID string) (*protos.Transaction, error) {
	return ledger.blockchain.getTransactionByID(txID)
}

// PutRawBlock puts a raw block on the chain. This function should only be
// used for synchronization between peers.
func (ledger *Ledger) PutRawBlock(block *protos.Block, blockNumber uint64) error {
	err := ledger.blockchain.persistRawBlock(block, blockNumber)
	if err != nil {
		return err
	}
	sendProducerBlockEvent(block)
	return nil
}

// VerifyChain will verify the integrity of the blockchain. This is accomplished
// by ensuring that the previous block hash stored in each block matches
// the actual hash of the previous block in the chain. The return value is the
// block number of lowest block in the range which can be verified as valid.
// The first block is assumed to be valid, and an error is only returned if the
// first block does not exist, or some other sort of irrecoverable ledger error
// such as the first block failing to hash is encountered.
// For example, if VerifyChain(0, 99) is called and previous hash values stored
// in blocks 8, 32, and 42 do not match the actual hashes of respective previous
// block 42 would be the return value from this function.
// highBlock is the high block in the chain to include in verification. If you
// wish to verify the entire chain, use ledger.GetBlockchainSize() - 1.
// lowBlock is the low block in the chain to include in verification. If
// you wish to verify the entire chain, use 0 for the genesis block.
func (ledger *Ledger) VerifyChain(highBlock, lowBlock uint64) (uint64, error) {
	panic("Not implemented")
	if highBlock >= ledger.GetBlockchainSize() {
		return highBlock, ErrOutOfBounds
	}
	if highBlock < lowBlock {
		return lowBlock, ErrOutOfBounds
	}

	currentBlock, err := ledger.GetBlockByNumber(highBlock)
	if err != nil {
		return highBlock, fmt.Errorf("Error fetching block %d.", highBlock)
	}
	if currentBlock == nil {
		return highBlock, fmt.Errorf("Block %d is nil.", highBlock)
	}

	for i := highBlock; i > lowBlock; i-- {
		previousBlock, err := ledger.GetBlockByNumber(i - 1)
		if err != nil {
			return i, nil
		}
		if previousBlock == nil {
			return i, nil
		}
		previousBlockHash, err := previousBlock.GetHash()
		if err != nil {
			return i, nil
		}
		if bytes.Compare(previousBlockHash, currentBlock.PreviousBlockHash) != 0 {
			return i, nil
		}
		currentBlock = previousBlock
	}

	return lowBlock, nil
}

func (ledger *Ledger) checkValidIDBegin() error {
	if ledger.currentID != nil {
		return fmt.Errorf("Another TxGroup [%s] already in-progress", ledger.currentID)
	}
	return nil
}

func (ledger *Ledger) checkValidIDCommitORRollback(id interface{}) error {
	if !reflect.DeepEqual(ledger.currentID, id) {
		return fmt.Errorf("Another TxGroup [%s] already in-progress", ledger.currentID)
	}
	return nil
}

func (ledger *Ledger) resetForNextTxGroup(txCommited bool) {
	ledgerLogger.Debug("resetting ledger state for next transaction batch")
	ledger.currentID = nil
	ledger.state.ClearInMemoryChanges(txCommited)
}

func sendProducerBlockEvent(block *protos.Block) {

	// Remove payload from deploy transactions. This is done to make block
	// events more lightweight as the payload for these types of transactions
	// can be very large.
	blockTransactions := block.GetTransactions()
	for _, transaction := range blockTransactions {
		if transaction.Type == protos.Transaction_CHAINCODE_DEPLOY {
			deploymentSpec := &protos.ChaincodeDeploymentSpec{}
			err := proto.Unmarshal(transaction.Payload, deploymentSpec)
			if err != nil {
				ledgerLogger.Errorf("Error unmarshalling deployment transaction for block event: %s", err)
				continue
			}
			deploymentSpec.CodePackage = nil
			deploymentSpecBytes, err := proto.Marshal(deploymentSpec)
			if err != nil {
				ledgerLogger.Errorf("Error marshalling deployment transaction for block event: %s", err)
				continue
			}
			transaction.Payload = deploymentSpecBytes
		}
	}

	producer.Send(producer.CreateBlockEvent(block))
}

//send chaincode events created by transactions
func sendChaincodeEvents(trs []*protos.TransactionResult) {
	if trs != nil {
		for _, tr := range trs {
			//we store empty chaincode events in the protobuf repeated array to make protobuf happy.
			//when we replay off a block ignore empty events
			if tr.ChaincodeEvent != nil && tr.ChaincodeEvent.ChaincodeID != "" {
				producer.Send(producer.CreateChaincodeEvent(tr.ChaincodeEvent))
			}
		}
	}
}
