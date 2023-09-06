#include <cassert>

#include "leveldb/db.h"
// #include "leveldb/options.h"

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());

  std::string value;
  status = db->Get(leveldb::ReadOptions(), "abc", &value);
  if (status.ok()) {
    status = db->Delete(leveldb::WriteOptions(), "abc");
    assert(status.ok());
  }

  status = db->Put(leveldb::WriteOptions(), "abc", "def");
  assert(status.ok());

  status = db->Get(leveldb::ReadOptions(), "abc", &value);
  assert(status.ok());
  assert(value == "def");

  status = db->Delete(leveldb::WriteOptions(), "abc");
  assert(status.ok());

  delete db;
}