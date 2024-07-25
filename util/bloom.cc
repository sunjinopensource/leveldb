// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

#include "util/hash.h"

namespace leveldb {

namespace {
static uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

class BloomFilterPolicy : public FilterPolicy {
 public:
  // bits_per_key 即 m/n，其中 m=位组长度，n=key数量
  explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
    // 算出最小误判率时的 k，等于 ln(2) * m/n，也就是 0.69 * bits_per_key
    // 故意舍入，以减少探测开销
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 ≈ ln(2)

    // 将k限制到[1~30]，太大的k会增加探测开销
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  const char* Name() const override { return "leveldb.BuiltinBloomFilter2"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    // 1. 计算位图长度
    // 1.1 计算位图长度m
    size_t bits = n * bits_per_key_;  // n * m / n -> m

    // 1.2 通过给 m 一个最小长度于64，来减少误判率
    if (bits < 64) bits = 64;

    // 1.3 计算表达此位图所需的字节数
    size_t bytes = (bits + 7) / 8;

    // 1.4 根据字节数重算 m
    bits = bytes * 8;

    // 2. 准备存储空间
    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0);  // 扩容空间，保持原有的过滤器数据不变

    // 3. 在位图尾部添加k值，使用KeyMayMatch时会读出此k值
    // 这样每个过滤器都可以有自己的k值
    dst->push_back(static_cast<char>(k_));

    char* array = &(*dst)[init_size];  // 得到存储区指针

    // 4. 依次将每个key加入过滤器
    for (int i = 0; i < n; i++) {
      // 用 double-hashing 产生一堆hash值（见[Kirsch,Mitzenmacher 2006]）
      // 好处是：不需要k个哈希函数，而在第一次的hash值上做k次增量得到k个hash值
      uint32_t h = BloomHash(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits

      // 为当前key在位图上打k个点
      for (size_t j = 0; j < k_; j++) {
        const uint32_t bitpos = h % bits;          // 计算点位
        array[bitpos / 8] |= (1 << (bitpos % 8));  // 打点
        h += delta;                                // 更新hash
      }
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {
    const size_t len = bloom_filter.size();
    if (len < 2) return false;  // 这里应该是9字节

    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;  // -1表示去掉尾部的k

    // 读出k
    const size_t k = array[len - 1];
    if (k > 30) {
      return true;
    }

    // 用和CreateFilter相同的方式打k个点，并检测
    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0)
        return false;  // 此key绝对不存在
      h += delta;
    }
    return true;  // 此key可能存在
  }

 private:
  size_t bits_per_key_;
  size_t k_;
};
}  // namespace

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilterPolicy(bits_per_key);
}

}  // namespace leveldb
