/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "sort.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

namespace {

constexpr size_t kRadixThreshold = 256;

template <typename T>
const T *ptr(const NsCol &c) {
  return static_cast<const T *>(c.data);
}

bool is_int_key(NsType t) { return t == NS_I32 || t == NS_I64; }

template <typename T>
bool try_sort_t(const T *keys, uint32_t *perm, size_t n) {
  if (n <= 1) return true;
  size_t inversions = 0;
  size_t inv_i = 0;
  for (size_t i = 1; i < n; ++i) {
    if (keys[perm[i - 1]] > keys[perm[i]]) {
      ++inversions;
      inv_i = i;
      if (inversions > 1) return false;
    }
  }
  if (inversions == 0) return true;
  std::swap(perm[inv_i - 1], perm[inv_i]);
  for (size_t i = 1; i < n; ++i) {
    if (keys[perm[i - 1]] > keys[perm[i]]) {
      std::swap(perm[inv_i - 1], perm[inv_i]);
      return false;
    }
  }
  return true;
}

bool try_sort(const NsCol &col, uint32_t *perm, size_t n) {
  switch (col.type) {
    case NS_I32:
      return try_sort_t(ptr<int32_t>(col), perm, n);
    case NS_I64:
      return try_sort_t(ptr<int64_t>(col), perm, n);
    case NS_F64:
      return try_sort_t(ptr<double>(col), perm, n);
    case NS_BOOL:
      return try_sort_t(ptr<uint8_t>(col), perm, n);
  }
  return false;
}

template <typename T>
void cmp_sort_t(const T *keys, uint32_t *perm, size_t n) {
  std::sort(perm, perm + n, [keys](uint32_t a, uint32_t b) { return keys[a] < keys[b]; });
}

void cmp_sort(const NsCol &col, uint32_t *perm, size_t n) {
  switch (col.type) {
    case NS_I32:
      cmp_sort_t(ptr<int32_t>(col), perm, n);
      break;
    case NS_I64:
      cmp_sort_t(ptr<int64_t>(col), perm, n);
      break;
    case NS_F64:
      cmp_sort_t(ptr<double>(col), perm, n);
      break;
    case NS_BOOL:
      cmp_sort_t(ptr<uint8_t>(col), perm, n);
      break;
  }
}

template <typename UKey>
struct KeyIdx {
  UKey key;
  uint32_t idx;
};

/* LSD radix, 8-bit passes. Even pass counts leave the result in `a`. */
template <typename UKey>
void lsd_radix(KeyIdx<UKey> *a, KeyIdx<UKey> *tmp, size_t n) {
  constexpr int n_passes = static_cast<int>(sizeof(UKey));
  KeyIdx<UKey> *cur = a;
  KeyIdx<UKey> *nxt = tmp;
  for (int pass = 0; pass < n_passes; ++pass) {
    size_t cnt[256] = {};
    const int shift = pass * 8;
    for (size_t i = 0; i < n; ++i) {
      ++cnt[(cur[i].key >> shift) & 0xFFu];
    }
    size_t sum = 0;
    for (int b = 0; b < 256; ++b) {
      const size_t c = cnt[b];
      cnt[b] = sum;
      sum += c;
    }
    for (size_t i = 0; i < n; ++i) {
      const unsigned b = static_cast<unsigned>((cur[i].key >> shift) & 0xFFu);
      nxt[cnt[b]++] = cur[i];
    }
    KeyIdx<UKey> *sw = cur;
    cur = nxt;
    nxt = sw;
  }
  if (cur != a) {
    std::memcpy(a, cur, n * sizeof(KeyIdx<UKey>));
  }
}

void radix_i32(const int32_t *keys, uint32_t *perm, size_t n) {
  std::vector<KeyIdx<uint32_t>> pairs(n);
  std::vector<KeyIdx<uint32_t>> tmp(n);
  for (size_t i = 0; i < n; ++i) {
    const uint32_t ukey = static_cast<uint32_t>(keys[perm[i]]) ^ 0x80000000u;
    pairs[i].key = ukey;
    pairs[i].idx = perm[i];
  }
  lsd_radix(pairs.data(), tmp.data(), n);
  for (size_t i = 0; i < n; ++i) perm[i] = pairs[i].idx;
}

void radix_i64(const int64_t *keys, uint32_t *perm, size_t n) {
  std::vector<KeyIdx<uint64_t>> pairs(n);
  std::vector<KeyIdx<uint64_t>> tmp(n);
  for (size_t i = 0; i < n; ++i) {
    const uint64_t ukey = static_cast<uint64_t>(keys[perm[i]]) ^ 0x8000000000000000ULL;
    pairs[i].key = ukey;
    pairs[i].idx = perm[i];
  }
  lsd_radix(pairs.data(), tmp.data(), n);
  for (size_t i = 0; i < n; ++i) perm[i] = pairs[i].idx;
}

void radix_sort(const NsCol &col, uint32_t *perm, size_t n) {
  if (col.type == NS_I32) {
    radix_i32(ptr<int32_t>(col), perm, n);
  } else {
    radix_i64(ptr<int64_t>(col), perm, n);
  }
}

void sort_range(const NsCol &col, uint32_t *perm, size_t n) {
  if (n <= 1) return;
  if (try_sort(col, perm, n)) return;
  if (is_int_key(col.type) && n >= kRadixThreshold) {
    radix_sort(col, perm, n);
  } else {
    cmp_sort(col, perm, n);
  }
}

bool cell_equal(const NsCol &c, uint32_t i, uint32_t j) {
  switch (c.type) {
    case NS_I32: {
      const int32_t *p = ptr<int32_t>(c);
      return p[i] == p[j];
    }
    case NS_I64: {
      const int64_t *p = ptr<int64_t>(c);
      return p[i] == p[j];
    }
    case NS_F64: {
      const double *p = ptr<double>(c);
      return p[i] == p[j];
    }
    case NS_BOOL: {
      const uint8_t *p = ptr<uint8_t>(c);
      return p[i] == p[j];
    }
  }
  return false;
}

bool prefix_equal(const NsCol *keys, int n_prefix, uint32_t r0, uint32_t r1) {
  for (int k = 0; k < n_prefix; ++k) {
    if (!cell_equal(keys[k], r0, r1)) return false;
  }
  return true;
}

/* Re-sort only inside ranges that are equal on keys[0 .. key_idx). */
void update_permutation(const NsCol *keys, int key_idx, uint32_t *perm, int32_t n_rows) {
  size_t i = 0;
  const size_t n = static_cast<size_t>(n_rows);
  while (i < n) {
    size_t j = i + 1;
    while (j < n && prefix_equal(keys, key_idx, perm[i], perm[j])) ++j;
    if (j - i > 1) sort_range(keys[key_idx], perm + i, j - i);
    i = j;
  }
}

} // namespace

int ns_sort_permutation(const NsCol *keys, int n_keys, int32_t n_rows, uint32_t *perm) {
  if (n_rows < 0) return -1;
  if (n_rows > 0 && perm == nullptr) return -1;
  if (n_keys < 0) return -1;
  if (n_keys > 0 && keys == nullptr) return -1;
  for (int k = 0; k < n_keys; ++k) {
    if (keys[k].data == nullptr && n_rows > 0) return -1;
  }

  for (int32_t r = 0; r < n_rows; ++r) perm[r] = static_cast<uint32_t>(r);
  if (n_rows <= 1 || n_keys == 0) return 0;

  sort_range(keys[0], perm, static_cast<size_t>(n_rows));
  for (int k = 1; k < n_keys; ++k) {
    update_permutation(keys, k, perm, n_rows);
  }
  return 0;
}
