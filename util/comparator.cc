// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() = default;

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() = default;

  const char* Name() const override { return "leveldb.BytewiseComparator"; }

  // 逐字节比较
  int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }

  // 假设 start 为 abcdg，limit 为 abef
  // 结果：start -> abd
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    size_t min_length = std::min(start->size(), limit.size());

    // 找到第一个不同的位置（换言之，找到公共前缀）
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // 若一个是另一个的前缀，Do not shorten 
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      // 假设 start 为 abcdg，limit 为 abef
      // 则 diff_index 为 2，diff_byte 为 c
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {  // c + 1 -> d < e
        (*start)[diff_index]++;  // start -> abddg
        start->resize(diff_index + 1);  // start -> abd
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb
