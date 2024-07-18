// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // 将slice拆分为若干片段写入Writable
  Status s;
  bool begin = true;
  do {
    // 剩余空间不足以写入片段头时，将剩余空间写入全0
    const int leftover = kBlockSize - block_offset_;  // block中的剩余空间
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {  
      if (leftover > 0) {
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;  // 开始新的block
    }

    // 计算下片段的长度和类型
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    assert(avail >= 0);
    
    // 片段长度
    const size_t fragment_length = (left < avail) ? left : avail;

    // 片段类型
    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    // 真正写入片段（ptr -> ptr+fragment_length）
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // 构造header
  char buf[kHeaderSize];
  // 1）构造CRC校验和：4字节
  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);
  // 2）构造长度：2字节
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  // 3）构造类型：1字节
  buf[6] = static_cast<char>(t);

  // 1. 向目标文件写入头
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    // 2. 写数据
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush();  // 3. 写盘
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
