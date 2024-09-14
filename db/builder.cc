// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

// 通过 MemTable(iter) 落地为 sst 文件，并输出 meta 信息（如 file_size、最小key、最大key）
// 文件按 meta->number 命名
// 若 iter 为空，则 meta->file_size 置0，且不生成 sst 文件
Status BuildTable(const std::string& dbname, Env* env, const Options& options, 
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    // 创建一个新的写文件
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    // 创建一个TableBuilder
    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());  // memtable的第一个key是最小的
    
    // 把所有kv一次性加入TableBuilder
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }

    if (!key.empty()) {
      meta->largest.DecodeFrom(key);  // memtable的最后一个key为最大的
    }

    // 完成 sst 文件尾部 block 的写入
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
