%module ustore
%include <std_map.i>
%include <std_string.i>
%include <std_pair.i>
%include <std_vector.i>

%{
#include "db.h"
%}

%template(PairStatusString) std::pair<ustore_kvdb::Status, std::string>;
%template(PairStr) std::pair<std::string, std::string>;
%template(VecStr) std::vector<std::string>;
%template(VectorPairStr)  std::vector<std::pair<std::string, std::string>>;
%template(PairStatusVectorPairStr) std::pair<ustore_kvdb::Status, std::vector<std::pair<std::string, std::string>>>;

namespace ustore_kvdb {
%nodefaultctor Iterator;
class Iterator;

class KVDB {
 public:
  explicit KVDB(unsigned int id = 42, const std::string& cfname = "default");

  std::pair<Status, std::string> Get(const std::string& key);
  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Write(WriteBatch* updates);
  bool Exist(const std::string& key);
  Iterator* NewIterator();
  size_t GetSize();
  std::string GetCFName();
  void OutputChunkStorage();

  Status InitGlobalState();
  std::pair<Status, std::string> Commit();
  std::pair<Status, std::string> GetState(const std::string& key);
  bool PutState(const std::string& key, const std::string& val,
                const std::string& txnID, const std::vector<std::string>& deps);
  std::pair<Status, std::string> GetBlock(const std::string& key, const std::string& version);
  std::pair<Status, std::string> PutBlock(const std::string& key, const std::string& value);

  std::pair<Status, std::string> GetHistoricalState(const std::string& key, unsigned long long blk_idx);

  std::pair<Status, std::string> GetHistoricalState(const std::string& key,
                                                    const std::string& uuid);

  std::pair<Status, std::string> GetTxnID(const std::string& key, unsigned long long blk_idx);

  std::pair<Status, std::string> GetTxnID(const std::string& key, const std::string& uuid);

  std::pair<ustore_kvdb::Status, std::vector<std::pair<std::string, std::string>>>
    GetDeps(const std::string& key, unsigned long long blk_idx);

  std::pair<ustore_kvdb::Status, std::vector<std::pair<std::string, std::string>>>
    GetDeps(const std::string& key, const std::string& uuid);
};

class Iterator {
 public:
  virtual ~Iterator();
  
  int GetTime();
  void Release();
  virtual void SetRange(const std::string& a, const std::string& b);
  virtual bool Valid();
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Seek(const std::string& key);
  virtual bool Next();
  virtual bool Prev();
  virtual std::string key();
  virtual std::string value();
};

extern Iterator* NewEmptyIterator();

}
