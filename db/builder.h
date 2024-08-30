// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "leveldb/status.h"

namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

// 通过 iter(实际就是MemTable) 构建 sst 文件，并输出sst的相关元数据到meta（如file_size等）
// 文件按meta->number命名
// 若 iter 为空，则 meta->file_size 置0，并且不会生成表文件
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_
