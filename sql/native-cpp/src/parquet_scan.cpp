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

#include "parquet_scan.h"

NsHdfsSizeFn g_hdfs_size = nullptr;
NsHdfsPreadFn g_hdfs_pread = nullptr;

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/bloom_filter.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#if defined(__linux__)
#include <dlfcn.h>
#include <fcntl.h>
#endif

namespace {

thread_local std::vector<std::pair<int64_t, std::string>> g_strdict;
thread_local std::unordered_set<int64_t> g_seen;
std::mutex g_pq_mu;
const size_t kMaxStrDict = 1000000;

int32_t mix_k1(int32_t k1) {
  k1 *= static_cast<int32_t>(0xcc9e2d51);
  k1 = static_cast<int32_t>(static_cast<uint32_t>(k1) << 15 |
                            static_cast<uint32_t>(k1) >> 17);
  k1 *= static_cast<int32_t>(0x1b873593);
  return k1;
}

int32_t mix_h1(int32_t h1, int32_t k1) {
  h1 ^= k1;
  h1 = static_cast<int32_t>(static_cast<uint32_t>(h1) << 13 |
                            static_cast<uint32_t>(h1) >> 19);
  return h1 * 5 + static_cast<int32_t>(0xe6546b64);
}

int32_t fmix(int32_t h1, int32_t len) {
  h1 ^= len;
  h1 ^= static_cast<int32_t>(static_cast<uint32_t>(h1) >> 16);
  h1 *= static_cast<int32_t>(0x85ebca6b);
  h1 ^= static_cast<int32_t>(static_cast<uint32_t>(h1) >> 13);
  h1 *= static_cast<int32_t>(0xc2b2ae35);
  h1 ^= static_cast<int32_t>(static_cast<uint32_t>(h1) >> 16);
  return h1;
}

/* Spark Murmur3_x86_32.hashUnsafeBytes (pre-2.3 variant), seed 42, LE ints. */
int32_t spark_murmur32(const uint8_t *p, int32_t n) {
  int32_t aligned = n - (n % 4);
  int32_t h1 = 42;
  for (int32_t i = 0; i < aligned; i += 4) {
    int32_t half;
    std::memcpy(&half, p + i, 4);
    h1 = mix_h1(h1, mix_k1(half));
  }
  for (int32_t i = aligned; i < n; ++i) {
    int32_t halfWord = static_cast<int8_t>(p[i]);
    h1 = mix_h1(h1, mix_k1(halfWord));
  }
  return fmix(h1, n);
}

uint64_t spark_prefix(const uint8_t *p, int n) {
  if (n <= 0) return 0;
  uint64_t raw = 0;
  if (n >= 8) {
    std::memcpy(&raw, p, 8);
  } else if (n > 4) {
    std::memcpy(&raw, p, static_cast<size_t>(n));
  } else {
    int32_t pRaw = 0;
    std::memcpy(&pRaw, p, static_cast<size_t>(n));
    raw = static_cast<uint64_t>(static_cast<uint32_t>(pRaw));
  }
  uint64_t swapped = __builtin_bswap64(raw);
  if (n >= 8) return swapped;
  uint64_t mask = (1ULL << (8 - n) * 8) - 1;
  return swapped & ~mask;
}

int64_t hash64_utf8(const uint8_t *p, int n) {
  if (p == nullptr || n <= 0) return 0;
  int64_t h = (static_cast<int64_t>(spark_murmur32(p, n)) << 32) ^
              (static_cast<int64_t>(static_cast<uint64_t>(n + 1) << 17 |
                                    static_cast<uint64_t>(n + 1) >> 47) ^
               static_cast<int64_t>(spark_prefix(p, n)));
  return h == 0 ? 1 : h;
}

void remember(int64_t h, const uint8_t *p, int n) {
  if (h == 0 || p == nullptr || n < 0) return;
  if (g_strdict.size() >= kMaxStrDict) return;
  if (!g_seen.insert(h).second) return;
  g_strdict.emplace_back(h, std::string(reinterpret_cast<const char *>(p), static_cast<size_t>(n)));
}

void append_col(NsBatch *out, int c, NsType t, const void *src, int n, int dst_off) {
  if (out == nullptr || out->cols == nullptr || c < 0 || c >= out->n_cols) return;
  if (out->cols[c].data == nullptr) {
    size_t es = t == NS_F64 ? sizeof(double) : t == NS_BOOL ? 1 : sizeof(int64_t);
    out->cols[c].data = std::calloc(static_cast<size_t>(out->n_rows), es);
    out->cols[c].type = t == NS_I32 ? NS_I64 : t;
    out->cols[c].n_rows = out->n_rows;
  }
  if (src == nullptr || n <= 0) return;
  if (dst_off < 0 || n < 0 || dst_off > out->n_rows || n > out->n_rows - dst_off) return;
  if (t == NS_F64) {
    std::memcpy(static_cast<double *>(out->cols[c].data) + dst_off, src,
                sizeof(double) * static_cast<size_t>(n));
  } else if (t == NS_BOOL) {
    std::memcpy(static_cast<uint8_t *>(out->cols[c].data) + dst_off, src,
                static_cast<size_t>(n));
  } else {
    std::memcpy(static_cast<int64_t *>(out->cols[c].data) + dst_off, src,
                sizeof(int64_t) * static_cast<size_t>(n));
  }
}

int64_t dict_index(const arrow::Array &idx, int r) {
  switch (idx.type_id()) {
    case arrow::Type::INT8:
      return static_cast<const arrow::Int8Array &>(idx).Value(r);
    case arrow::Type::INT16:
      return static_cast<const arrow::Int16Array &>(idx).Value(r);
    case arrow::Type::INT32:
      return static_cast<const arrow::Int32Array &>(idx).Value(r);
    case arrow::Type::INT64:
      return static_cast<const arrow::Int64Array &>(idx).Value(r);
    case arrow::Type::UINT8:
      return static_cast<const arrow::UInt8Array &>(idx).Value(r);
    case arrow::Type::UINT16:
      return static_cast<const arrow::UInt16Array &>(idx).Value(r);
    case arrow::Type::UINT32:
      return static_cast<const arrow::UInt32Array &>(idx).Value(r);
    default:
      return -1;
  }
}

bool names_eq_ci(const char *a, const char *b) {
  if (!a || !b) return false;
  while (*a && *b) {
    if (std::tolower(static_cast<unsigned char>(*a)) !=
        std::tolower(static_cast<unsigned char>(*b))) {
      return false;
    }
    ++a;
    ++b;
  }
  return *a == 0 && *b == 0;
}

/* ReadRowGroups column_indices are parquet LEAF indexes, not Arrow fields. */
int leaf_index_ci(const parquet::SchemaDescriptor *d, const char *name) {
  if (!d || !name || !name[0]) return -1;
  for (int i = 0; i < d->num_columns(); ++i) {
    const parquet::ColumnDescriptor *col = d->Column(i);
    if (names_eq_ci(col->name().c_str(), name)) return i;
    const auto pth = col->path();
    if (!pth) continue;
    const std::string path = pth->ToDotString();
    if (names_eq_ci(path.c_str(), name)) return i;
    const size_t dot = path.rfind('.');
    if (dot != std::string::npos && names_eq_ci(path.c_str() + dot + 1, name)) {
      return i;
    }
  }
  return -1;
}

int64_t row_group_start(const parquet::RowGroupMetaData *rg) {
  if (!rg) return 0;
  const int64_t off = rg->file_offset();
  if (off > 0) return off;
  int64_t best = 0;
  bool any = false;
  for (int c = 0; c < rg->num_columns(); ++c) {
    std::unique_ptr<parquet::ColumnChunkMetaData> cc = rg->ColumnChunk(c);
    if (!cc) continue;
    int64_t cand = 0;
    if (cc->has_dictionary_page() && cc->dictionary_page_offset() > 0) {
      cand = cc->dictionary_page_offset();
    } else if (cc->data_page_offset() > 0) {
      cand = cc->data_page_offset();
    } else if (cc->file_offset() > 0) {
      cand = cc->file_offset();
    }
    if (cand > 0 && (!any || cand < best)) {
      best = cand;
      any = true;
    }
  }
  return any ? best : 0;
}

bool rg_overlaps(int64_t off, int64_t sz, int64_t start, int64_t length, bool known) {
  if (length <= 0) return true;
  if (!known) return start == 0;
  return off < start + length && off + sz > start;
}

bool pred_ok_stats(const parquet::Statistics *st, const NsColPred &pred) {
  if (st == nullptr || !st->HasMinMax()) return true;
  int64_t mn = 0;
  int64_t mx = 0;
  switch (st->physical_type()) {
    case parquet::Type::INT32: {
      auto *t = dynamic_cast<const parquet::TypedStatistics<parquet::Int32Type> *>(st);
      if (t == nullptr) return true;
      mn = t->min();
      mx = t->max();
      break;
    }
    case parquet::Type::INT64: {
      auto *t = dynamic_cast<const parquet::TypedStatistics<parquet::Int64Type> *>(st);
      if (t == nullptr) return true;
      mn = t->min();
      mx = t->max();
      break;
    }
    case parquet::Type::BOOLEAN: {
      auto *t = dynamic_cast<const parquet::TypedStatistics<parquet::BooleanType> *>(st);
      if (t == nullptr) return true;
      mn = t->min() ? 1 : 0;
      mx = t->max() ? 1 : 0;
      break;
    }
    default:
      return true;
  }
  switch (pred.op) {
    case 1:
      return pred.value >= mn && pred.value <= mx;
    case 2:
      return mx >= pred.value;
    case 3:
      return mn <= pred.value;
    case 4:
      return mx > pred.value;
    case 5:
      return mn < pred.value;
    default:
      return true;
  }
}

bool bloom_excludes_eq(arrow::io::RandomAccessFile *file,
                      const parquet::ColumnChunkMetaData *cc, int64_t value) {
  if (file == nullptr || cc == nullptr) return false;
  std::optional<int64_t> off = cc->bloom_filter_offset();
  if (!off.has_value() || *off <= 0) return false;
  try {
    int64_t len = 256 * 1024;
    std::optional<int64_t> blen = cc->bloom_filter_length();
    if (blen.has_value() && *blen > 0) len = *blen;
    auto maybe = file->ReadAt(*off, len);
    if (!maybe.ok() || !*maybe) return false;
    arrow::io::BufferReader br(*maybe);
    parquet::BlockSplitBloomFilter bf = parquet::BlockSplitBloomFilter::Deserialize(
        parquet::default_reader_properties(), &br, blen);
    uint64_t h = 0;
    switch (cc->type()) {
      case parquet::Type::INT32:
        h = bf.Hash(static_cast<int32_t>(value));
        break;
      case parquet::Type::INT64:
        h = bf.Hash(value);
        break;
      default:
        return false;
    }
    return !bf.FindHash(h);
  } catch (...) {
    return false;
  }
}

bool rg_survives_preds(const parquet::RowGroupMetaData *rg, const NsFileScan &scan,
                      const std::vector<int> &wanted,
                      arrow::io::RandomAccessFile *file, int *bloom_hits) {
  if (rg == nullptr || scan.n_preds <= 0 || scan.preds == nullptr) return true;
  for (int32_t i = 0; i < scan.n_preds; ++i) {
    const NsColPred &pred = scan.preds[i];
    if (pred.col < 0 || pred.col >= scan.n_cols) continue;
    const int leaf = wanted[static_cast<size_t>(pred.col)];
    if (leaf < 0 || leaf >= rg->num_columns()) continue;
    auto cc = rg->ColumnChunk(leaf);
    if (!cc) continue;
    auto st = cc->statistics();
    if (st && !pred_ok_stats(st.get(), pred)) return false;
    (void)file;
    (void)bloom_hits;
  }
  return true;
}

#if defined(__linux__)
typedef int32_t tSize;
typedef int64_t tOffset;
typedef uint16_t tPort;
typedef int64_t tTime;
enum tObjectKind { kObjectKindFile = 'F', kObjectKindDirectory = 'D' };
struct hdfsFileInfo {
  tObjectKind mKind;
  char *mName;
  tTime mLastMod;
  tOffset mSize;
  short mReplication;
  tOffset mBlockSize;
  char *mOwner;
  char *mGroup;
  short mPermissions;
  tTime mLastAccess;
};
typedef void *hdfsFS;
typedef void *hdfsFile;
typedef void *hdfsBuilder;

struct HdfsApi {
  void *handle = nullptr;
  hdfsBuilder *(*NewBuilder)() = nullptr;
  void (*BuilderSetNameNode)(hdfsBuilder *, const char *) = nullptr;
  void (*BuilderSetNameNodePort)(hdfsBuilder *, tPort) = nullptr;
  hdfsFS (*BuilderConnect)(hdfsBuilder *) = nullptr;
  hdfsFS (*ConnectNewInstance)(const char *, tPort) = nullptr;
  hdfsFile (*OpenFile)(hdfsFS, const char *, int, int, short, tSize) = nullptr;
  int (*CloseFile)(hdfsFS, hdfsFile) = nullptr;
  tSize (*Pread)(hdfsFS, hdfsFile, tOffset, void *, tSize) = nullptr;
  hdfsFileInfo *(*GetPathInfo)(hdfsFS, const char *) = nullptr;
  void (*FreeFileInfo)(hdfsFileInfo *, int) = nullptr;
};

HdfsApi &hdfs_api() {
  static HdfsApi api;
  static bool tried = false;
  if (tried) return api;
  tried = true;
  void *h = nullptr;
  const char *home = std::getenv("HADOOP_HOME");
  if (home != nullptr && home[0] != '\0') {
    std::string p = std::string(home) + "/lib/native/libhdfs.so";
    h = dlopen(p.c_str(), RTLD_NOW | RTLD_GLOBAL);
  }
  if (h == nullptr) h = dlopen("libhdfs.so", RTLD_NOW | RTLD_GLOBAL);
  const char *cands[] = {
      "/usr/lib/hadoop/lib/native/libhdfs.so",
      "/usr/odp/current/hadoop/lib/native/libhdfs.so",
      "/usr/odp/current/hadoop-client/lib/native/libhdfs.so",
      "/usr/odp/3.3.6.5-1009/hadoop/lib/native/libhdfs.so",
      nullptr};
  for (int i = 0; h == nullptr && cands[i] != nullptr; ++i) {
    h = dlopen(cands[i], RTLD_NOW | RTLD_GLOBAL);
  }
  if (h == nullptr) return api;
  api.handle = h;
  api.NewBuilder = reinterpret_cast<decltype(api.NewBuilder)>(dlsym(h, "hdfsNewBuilder"));
  api.BuilderSetNameNode =
      reinterpret_cast<decltype(api.BuilderSetNameNode)>(dlsym(h, "hdfsBuilderSetNameNode"));
  api.BuilderSetNameNodePort = reinterpret_cast<decltype(api.BuilderSetNameNodePort)>(
      dlsym(h, "hdfsBuilderSetNameNodePort"));
  api.BuilderConnect =
      reinterpret_cast<decltype(api.BuilderConnect)>(dlsym(h, "hdfsBuilderConnect"));
  api.ConnectNewInstance =
      reinterpret_cast<decltype(api.ConnectNewInstance)>(dlsym(h, "hdfsConnectNewInstance"));
  api.OpenFile = reinterpret_cast<decltype(api.OpenFile)>(dlsym(h, "hdfsOpenFile"));
  api.CloseFile = reinterpret_cast<decltype(api.CloseFile)>(dlsym(h, "hdfsCloseFile"));
  api.Pread = reinterpret_cast<decltype(api.Pread)>(dlsym(h, "hdfsPread"));
  api.GetPathInfo = reinterpret_cast<decltype(api.GetPathInfo)>(dlsym(h, "hdfsGetPathInfo"));
  api.FreeFileInfo = reinterpret_cast<decltype(api.FreeFileInfo)>(dlsym(h, "hdfsFreeFileInfo"));
  return api;
}

hdfsFS hdfs_connect(const HdfsApi &api, const char *uri) {
  static std::mutex mu;
  static std::unordered_map<std::string, hdfsFS> cache;
  std::string host;
  int port = 0;
  if (uri != nullptr && std::strncmp(uri, "hdfs://", 7) == 0) {
    const char *p = uri + 7;
    const char *slash = std::strchr(p, '/');
    std::string auth = slash ? std::string(p, slash) : std::string(p);
    const auto colon = auth.rfind(':');
    if (colon != std::string::npos) {
      host = auth.substr(0, colon);
      port = std::atoi(auth.substr(colon + 1).c_str());
    } else {
      host = auth;
      port = 8020;
    }
  }
  const std::string key = host.empty() ? std::string("default")
                                       : host + ":" + std::to_string(port);
  std::lock_guard<std::mutex> lk(mu);
  auto it = cache.find(key);
  if (it != cache.end()) return it->second;
  hdfsFS fs = nullptr;
  if (api.NewBuilder != nullptr && api.BuilderConnect != nullptr) {
    hdfsBuilder *b = api.NewBuilder();
    if (b != nullptr) {
      if (api.BuilderSetNameNode != nullptr) api.BuilderSetNameNode(b, "default");
      fs = api.BuilderConnect(b);
    }
  }
  if (fs == nullptr && api.NewBuilder != nullptr && !host.empty()) {
    hdfsBuilder *b = api.NewBuilder();
    if (b != nullptr) {
      if (api.BuilderSetNameNode != nullptr) api.BuilderSetNameNode(b, host.c_str());
      if (port > 0 && api.BuilderSetNameNodePort != nullptr) {
        api.BuilderSetNameNodePort(b, static_cast<tPort>(port));
      }
      fs = api.BuilderConnect(b);
    }
  }
  if (fs == nullptr && api.ConnectNewInstance != nullptr) {
    fs = api.ConnectNewInstance(host.empty() ? "default" : host.c_str(),
                                static_cast<tPort>(port));
  }
  if (fs != nullptr) cache[key] = fs;
  return fs;
}

std::string hdfs_fs_path(const char *uri) {
  if (uri == nullptr) return "";
  if (std::strncmp(uri, "hdfs://", 7) != 0) return uri;
  const char *slash = std::strchr(uri + 7, '/');
  return slash ? slash : "/";
}

class HdfsRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
  HdfsRandomAccessFile(const HdfsApi *api, hdfsFS fs, hdfsFile file, int64_t size)
      : api_(api), fs_(fs), file_(file), size_(size), pos_(0), closed_(false) {}
  ~HdfsRandomAccessFile() override { (void)Close(); }

  arrow::Status Close() override {
    if (!closed_ && file_ != nullptr && api_ != nullptr && api_->CloseFile != nullptr) {
      api_->CloseFile(fs_, file_);
      file_ = nullptr;
    }
    closed_ = true;
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }
  arrow::Result<int64_t> Tell() const override { return pos_; }
  arrow::Status Seek(int64_t position) override {
    if (closed_) return arrow::Status::IOError("hdfs closed");
    if (position < 0) return arrow::Status::Invalid("hdfs seek");
    pos_ = position;
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> GetSize() override { return size_; }
  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void *out) override {
    if (closed_ || file_ == nullptr || api_ == nullptr || api_->Pread == nullptr) {
      return arrow::Status::IOError("hdfs closed");
    }
    if (nbytes <= 0) return 0;
    tSize n = api_->Pread(fs_, file_, static_cast<tOffset>(position), out,
                          static_cast<tSize>(nbytes));
    if (n < 0) return arrow::Status::IOError("hdfsPread");
    return static_cast<int64_t>(n);
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                       int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t n, ReadAt(position, nbytes, buf->mutable_data()));
    if (n < nbytes) {
      ARROW_RETURN_NOT_OK(buf->Resize(n));
    }
    return std::shared_ptr<arrow::Buffer>(std::move(buf));
  }
  arrow::Result<int64_t> Read(int64_t nbytes, void *out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t n, ReadAt(pos_, nbytes, out));
    pos_ += n;
    return n;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buf, ReadAt(pos_, nbytes));
    pos_ += buf->size();
    return buf;
  }

 private:
  const HdfsApi *api_;
  hdfsFS fs_;
  hdfsFile file_;
  int64_t size_;
  int64_t pos_;
  bool closed_;
};
#endif

class JavaHdfsFile : public arrow::io::RandomAccessFile {
 public:
  JavaHdfsFile(std::string uri, int64_t size)
      : uri_(std::move(uri)), size_(size), pos_(0), closed_(false) {}
  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }
  arrow::Result<int64_t> Tell() const override { return pos_; }
  arrow::Status Seek(int64_t position) override {
    if (closed_) return arrow::Status::IOError("hdfs closed");
    if (position < 0) return arrow::Status::Invalid("hdfs seek");
    pos_ = position;
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> GetSize() override { return size_; }
  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void *out) override {
    if (closed_ || g_hdfs_pread == nullptr) return arrow::Status::IOError("hdfs closed");
    if (nbytes <= 0) return 0;
    const int64_t cap = 8 * 1024 * 1024;
    int64_t total = 0;
    auto *dst = static_cast<uint8_t *>(out);
    while (total < nbytes) {
      const int64_t n = std::min(cap, nbytes - total);
      const int64_t got = g_hdfs_pread(uri_.c_str(), position + total, dst + total, n);
      if (got < 0) return arrow::Status::IOError("hdfsPread java");
      if (got == 0) break;
      total += got;
      if (got < n) break;
    }
    return total;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
                                                       int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes < 0 ? 0 : nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t n, ReadAt(position, nbytes, buf->mutable_data()));
    if (n < nbytes) {
      ARROW_RETURN_NOT_OK(buf->Resize(n));
    }
    return std::shared_ptr<arrow::Buffer>(std::move(buf));
  }
  arrow::Result<int64_t> Read(int64_t nbytes, void *out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t n, ReadAt(pos_, nbytes, out));
    pos_ += n;
    return n;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buf, ReadAt(pos_, nbytes));
    pos_ += buf->size();
    return buf;
  }

 private:
  std::string uri_;
  int64_t size_;
  int64_t pos_;
  bool closed_;
};

bool is_hdfs_uri(const char *p) {
  return p != nullptr && std::strncmp(p, "hdfs:", 5) == 0;
}

std::string local_fs_path(const char *p) {
  if (p == nullptr) return "";
  if (std::strncmp(p, "file://", 7) == 0) return p + 7;
  return p;
}

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> open_scan_file(
    const char *path) {
  if (path == nullptr || path[0] == '\0') {
    return arrow::Status::Invalid("empty parquet path");
  }
  if (is_hdfs_uri(path)) {
    if (g_hdfs_size != nullptr && g_hdfs_pread != nullptr) {
      int64_t sz = g_hdfs_size(path);
      if (sz < 0) return arrow::Status::IOError("hdfsSize java failed");
      std::fprintf(stderr, "nativesql: pq hdfs=java path=%s size=%lld\n", path,
                   static_cast<long long>(sz));
      std::fflush(stderr);
      if (sz <= 0) return arrow::Status::IOError("hdfs empty");
      /* Dims and small files: one buffer. Fact files use seek+pread. */
      if (sz <= 16 * 1024 * 1024) {
        auto maybe = arrow::AllocateBuffer(sz);
        if (!maybe.ok() || !*maybe) {
          return arrow::Status::IOError("hdfs slurp alloc");
        }
        std::shared_ptr<arrow::Buffer> owned(std::move(*maybe));
        const int64_t chunk = 4 * 1024 * 1024;
        int64_t off = 0;
        while (off < sz) {
          const int64_t n = std::min(chunk, sz - off);
          const int64_t got =
              g_hdfs_pread(path, off, owned->mutable_data() + off, n);
          if (got != n) {
            return arrow::Status::IOError("hdfs slurp short read");
          }
          off += got;
        }
        return std::make_shared<arrow::io::BufferReader>(owned);
      }
      return std::shared_ptr<arrow::io::RandomAccessFile>(new JavaHdfsFile(path, sz));
    }
#if defined(__linux__)
    HdfsApi &api = hdfs_api();
    if (api.handle == nullptr || api.OpenFile == nullptr || api.Pread == nullptr) {
      return arrow::Status::IOError("libhdfs not loaded");
    }
    hdfsFS fs = hdfs_connect(api, path);
    if (fs == nullptr) return arrow::Status::IOError("hdfs connect failed");
    const std::string hp = hdfs_fs_path(path);
    hdfsFile hf = api.OpenFile(fs, hp.c_str(), O_RDONLY, 0, 0, 0);
    if (hf == nullptr) return arrow::Status::IOError("hdfs open failed: " + hp);
    int64_t sz = 0;
    if (api.GetPathInfo != nullptr) {
      hdfsFileInfo *info = api.GetPathInfo(fs, hp.c_str());
      if (info != nullptr) {
        sz = info->mSize;
        if (api.FreeFileInfo != nullptr) api.FreeFileInfo(info, 1);
      }
    }
    return std::shared_ptr<arrow::io::RandomAccessFile>(
        new HdfsRandomAccessFile(&api, fs, hf, sz));
#else
    return arrow::Status::NotImplemented("hdfs parquet open requires linux libhdfs");
#endif
  }
  return arrow::io::ReadableFile::Open(local_fs_path(path));
}

void fill_from_array(
    const arrow::Array &arr, NsType want, std::vector<int64_t> *i64,
    std::vector<double> *f64, std::vector<uint8_t> *b);

void fill_from_array(
    const arrow::Array &arr, NsType want, std::vector<int64_t> *i64,
    std::vector<double> *f64, std::vector<uint8_t> *b) {
  const int n = static_cast<int>(arr.length());
  if (want == NS_F64) {
    f64->assign(static_cast<size_t>(n), 0);
  } else if (want == NS_BOOL) {
    b->assign(static_cast<size_t>(n), 0);
  } else {
    i64->assign(static_cast<size_t>(n), 0);
  }
  if (arr.type_id() == arrow::Type::DICTIONARY) {
    const auto &d = static_cast<const arrow::DictionaryArray &>(arr);
    std::vector<int64_t> di;
    std::vector<double> df;
    std::vector<uint8_t> db;
    fill_from_array(*d.dictionary(), want, &di, &df, &db);
    for (int r = 0; r < n; ++r) {
      if (d.IsNull(r)) continue;
      const int64_t ix = dict_index(*d.indices(), r);
      if (ix < 0) continue;
      if (want == NS_F64) {
        if (ix >= static_cast<int64_t>(df.size())) continue;
        (*f64)[static_cast<size_t>(r)] = df[static_cast<size_t>(ix)];
      } else if (want == NS_BOOL) {
        if (ix >= static_cast<int64_t>(db.size())) continue;
        (*b)[static_cast<size_t>(r)] = db[static_cast<size_t>(ix)];
      } else {
        if (ix >= static_cast<int64_t>(di.size())) continue;
        (*i64)[static_cast<size_t>(r)] = di[static_cast<size_t>(ix)];
      }
    }
    return;
  }
  auto set_i = [&](int row, int64_t v) {
    if (want == NS_F64) (*f64)[static_cast<size_t>(row)] = static_cast<double>(v);
    else if (want == NS_BOOL) (*b)[static_cast<size_t>(row)] = v != 0 ? 1 : 0;
    else (*i64)[static_cast<size_t>(row)] = v;
  };
  auto set_f = [&](int row, double v) {
    if (want == NS_F64) (*f64)[static_cast<size_t>(row)] = v;
    else if (want == NS_BOOL) (*b)[static_cast<size_t>(row)] = v != 0 ? 1 : 0;
    else (*i64)[static_cast<size_t>(row)] = static_cast<int64_t>(v);
  };
  for (int r = 0; r < n; ++r) {
    if (arr.IsNull(r)) continue;
    switch (arr.type_id()) {
      case arrow::Type::INT8: {
        set_i(r, static_cast<const arrow::Int8Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::INT16: {
        set_i(r, static_cast<const arrow::Int16Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::INT32: {
        set_i(r, static_cast<const arrow::Int32Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::DATE32: {
        set_i(r, static_cast<const arrow::Date32Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::TIME32: {
        set_i(r, static_cast<const arrow::Time32Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::UINT8: {
        set_i(r, static_cast<const arrow::UInt8Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::UINT16: {
        set_i(r, static_cast<const arrow::UInt16Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::UINT32: {
        set_i(r, static_cast<const arrow::UInt32Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::INT64: {
        set_i(r, static_cast<const arrow::Int64Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::DATE64: {
        set_i(r, static_cast<const arrow::Date64Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::TIME64: {
        set_i(r, static_cast<const arrow::Time64Array &>(arr).Value(r));
        break;
      }
      case arrow::Type::DURATION: {
        set_i(r, static_cast<const arrow::DurationArray &>(arr).Value(r));
        break;
      }
      case arrow::Type::TIMESTAMP: {
        set_i(r, static_cast<const arrow::TimestampArray &>(arr).Value(r));
        break;
      }
      case arrow::Type::DOUBLE: {
        set_f(r, static_cast<const arrow::DoubleArray &>(arr).Value(r));
        break;
      }
      case arrow::Type::FLOAT: {
        set_f(r, static_cast<const arrow::FloatArray &>(arr).Value(r));
        break;
      }
      case arrow::Type::BOOL: {
        set_i(r, static_cast<const arrow::BooleanArray &>(arr).Value(r) ? 1 : 0);
        break;
      }
      case arrow::Type::STRING: {
        auto s = static_cast<const arrow::StringArray &>(arr).GetView(r);
        int64_t h = hash64_utf8(reinterpret_cast<const uint8_t *>(s.data()),
                                static_cast<int>(s.size()));
        set_i(r, h);
        remember(h, reinterpret_cast<const uint8_t *>(s.data()),
                 static_cast<int>(s.size()));
        break;
      }
      case arrow::Type::BINARY: {
        auto s = static_cast<const arrow::BinaryArray &>(arr).GetView(r);
        int64_t h = hash64_utf8(reinterpret_cast<const uint8_t *>(s.data()),
                                static_cast<int>(s.size()));
        set_i(r, h);
        remember(h, reinterpret_cast<const uint8_t *>(s.data()),
                 static_cast<int>(s.size()));
        break;
      }
      case arrow::Type::LARGE_STRING:
      case arrow::Type::LARGE_BINARY: {
        auto s = static_cast<const arrow::LargeBinaryArray &>(arr).GetView(r);
        const int nclip = s.size() > static_cast<size_t>(INT32_MAX)
                              ? INT32_MAX
                              : static_cast<int>(s.size());
        int64_t h = hash64_utf8(reinterpret_cast<const uint8_t *>(s.data()), nclip);
        set_i(r, h);
        remember(h, reinterpret_cast<const uint8_t *>(s.data()), nclip);
        break;
      }
      case arrow::Type::FIXED_SIZE_BINARY: {
        const auto &fb = static_cast<const arrow::FixedSizeBinaryArray &>(arr);
        const int w = fb.byte_width();
        const uint8_t *p = fb.GetValue(r);
        int64_t h = hash64_utf8(p, w);
        set_i(r, h);
        remember(h, p, w);
        break;
      }
      case arrow::Type::DECIMAL128: {
        const auto &d = static_cast<const arrow::Decimal128Array &>(arr);
        arrow::Decimal128 dec(d.GetValue(r));
        set_i(r, static_cast<int64_t>(dec.low_bits()));
        break;
      }
      default:
        if (want == NS_F64) {
          /* leave 0 */
        }
        break;
    }
  }
}

int read_one(const NsFileSplit &sp, const NsFileScan &scan, std::vector<std::vector<int64_t>> *i64,
             std::vector<std::vector<double>> *f64, std::vector<std::vector<uint8_t>> *b,
             int *nrows) {
  try {
    std::lock_guard<std::mutex> lock(g_pq_mu);
    std::shared_ptr<arrow::io::RandomAccessFile> infile;
    std::shared_ptr<arrow::Buffer> owned;
    if (sp.bytes != nullptr && sp.nbytes > 0) {
      /* Copy into an Arrow-owned aligned buffer. Wrapping a JNI pin aborts
       * when GetByteArrayElements returns null or the pin is unaligned. */
      auto maybe = arrow::AllocateBuffer(sp.nbytes);
      if (!maybe.ok() || !*maybe) return -1;
      owned = std::shared_ptr<arrow::Buffer>(std::move(*maybe));
      std::memcpy(owned->mutable_data(), sp.bytes, static_cast<size_t>(sp.nbytes));
      infile = std::make_shared<arrow::io::BufferReader>(owned);
    } else if (sp.path != nullptr && sp.path[0] != '\0') {
      std::fprintf(stderr,
                   "nativesql: pq open path=%s start=%lld len=%lld cols=%d "
                   "hdfs=%d preds=%d\n",
                   sp.path,
                   static_cast<long long>(sp.start),
                   static_cast<long long>(sp.length), scan.n_cols,
                   is_hdfs_uri(sp.path) ? 1 : 0, scan.n_preds);
      std::fflush(stderr);
      auto r = open_scan_file(sp.path);
      if (!r.ok()) {
        std::fprintf(stderr, "nativesql: pq open failed path=%s err=%s\n",
                     sp.path, r.status().ToString().c_str());
        std::fflush(stderr);
        return -1;
      }
      infile = *r;
    } else {
      return -1;
    }
    parquet::arrow::FileReaderBuilder builder;
    if (!builder.Open(infile, parquet::default_reader_properties()).ok()) return -1;
    builder.memory_pool(arrow::default_memory_pool());
    parquet::ArrowReaderProperties ap = parquet::default_arrow_reader_properties();
    ap.set_use_threads(false);
    ap.set_coerce_int96_timestamp_unit(arrow::TimeUnit::MICRO);
    builder.properties(ap);
    std::unique_ptr<parquet::arrow::FileReader> reader;
    if (!builder.Build(&reader).ok() || !reader) return -1;
    reader->set_use_threads(false);
    auto *pq = reader->parquet_reader();
    if (!pq) return -1;
    auto md = pq->metadata();
    if (!md) return -1;
    const parquet::SchemaDescriptor *descr = md->schema();
    const int nleaf = md->num_columns();
    std::vector<int> rgs;
    int64_t meta_rows = 0;
    bool known_off = false;
    for (int i = 0; i < md->num_row_groups(); ++i) {
      auto rg = md->RowGroup(i);
      const int64_t off = row_group_start(rg.get());
      if (off > 0) known_off = true;
      const int64_t sz = rg->total_byte_size();
      if (rg_overlaps(off, sz, sp.start, sp.length, known_off || off > 0)) {
        rgs.push_back(i);
        meta_rows += rg->num_rows();
      }
    }
    /* Second pass if we only learned offsets after the first groups. */
    if (known_off && sp.length > 0) {
      rgs.clear();
      meta_rows = 0;
      for (int i = 0; i < md->num_row_groups(); ++i) {
        auto rg = md->RowGroup(i);
        const int64_t off = row_group_start(rg.get());
        const int64_t sz = rg->total_byte_size();
        if (rg_overlaps(off, sz, sp.start, sp.length, true)) {
          rgs.push_back(i);
          meta_rows += rg->num_rows();
        }
      }
    }
    if (rgs.empty()) {
      /* Spark FilePartition ranges often miss every row group. Must still
       * return zero-width columns so the caller can concatenate splits. */
      i64->assign(static_cast<size_t>(scan.n_cols), {});
      f64->assign(static_cast<size_t>(scan.n_cols), {});
      b->assign(static_cast<size_t>(scan.n_cols), {});
      *nrows = 0;
      return 0;
    }
    std::vector<int> wanted(static_cast<size_t>(scan.n_cols), -1);
    std::vector<int> read_idx;
    read_idx.reserve(static_cast<size_t>(scan.n_cols));
    const bool have_names = scan.col_names != nullptr;
    for (int c = 0; c < scan.n_cols; ++c) {
      int found = -1;
      if (have_names && scan.col_names[c] && scan.col_names[c][0]) {
        found = leaf_index_ci(descr, scan.col_names[c]);
      } else if (!have_names && c < nleaf) {
        /* No name list at all: positional (in-memory / unit tests). */
        found = c;
      }
      /* Empty name is an intentional prune: do not read that leaf. */
      if (found < 0 || found >= nleaf) found = -1;
      wanted[static_cast<size_t>(c)] = found;
      if (found >= 0) read_idx.push_back(found);
    }
    i64->assign(static_cast<size_t>(scan.n_cols), {});
    f64->assign(static_cast<size_t>(scan.n_cols), {});
    b->assign(static_cast<size_t>(scan.n_cols), {});
    if (read_idx.empty()) {
      const int n = static_cast<int>(meta_rows > INT32_MAX ? INT32_MAX : meta_rows);
      *nrows = n;
      for (int c = 0; c < scan.n_cols; ++c) {
        (*i64)[static_cast<size_t>(c)].assign(static_cast<size_t>(n), 0);
        (*f64)[static_cast<size_t>(c)].assign(static_cast<size_t>(n), 0);
        (*b)[static_cast<size_t>(c)].assign(static_cast<size_t>(n), 0);
      }
      return 0;
    }
    std::sort(read_idx.begin(), read_idx.end());
    read_idx.erase(std::unique(read_idx.begin(), read_idx.end()), read_idx.end());
    if (scan.n_preds > 0 && scan.preds != nullptr) {
      std::vector<int> kept;
      kept.reserve(rgs.size());
      int64_t kept_rows = 0;
      int bloom_hits = 0;
      const int before = static_cast<int>(rgs.size());
      for (int rg : rgs) {
        auto rgm = md->RowGroup(rg);
        if (rg_survives_preds(rgm.get(), scan, wanted, infile.get(), &bloom_hits)) {
          kept.push_back(rg);
          if (rgm) kept_rows += rgm->num_rows();
        }
      }
      std::fprintf(stderr, "nativesql: pq skip_rg=%d keep=%zu bloom=%d preds=%d\n",
                   before - static_cast<int>(kept.size()), kept.size(), bloom_hits,
                   scan.n_preds);
      std::fflush(stderr);
      rgs.swap(kept);
      meta_rows = kept_rows;
      if (rgs.empty()) {
        i64->assign(static_cast<size_t>(scan.n_cols), {});
        f64->assign(static_cast<size_t>(scan.n_cols), {});
        b->assign(static_cast<size_t>(scan.n_cols), {});
        *nrows = 0;
        return 0;
      }
    }
    std::fprintf(stderr, "nativesql: pq read_cols=%zu of %d\n",
                 read_idx.size(), scan.n_cols);
    std::fflush(stderr);
    for (int idx : read_idx) {
      if (idx < 0 || idx >= nleaf) return -1;
    }
    int total = 0;
    for (int rg : rgs) {
      std::shared_ptr<arrow::Table> table;
      auto st = reader->ReadRowGroup(rg, read_idx, &table);
      if (!st.ok() || !table) {
        std::fprintf(stderr, "nativesql: pq rg_err rg=%d %s\n", rg,
                     st.ToString().c_str());
        std::fflush(stderr);
        return -1;
      }
      const int pn = static_cast<int>(table->num_rows());
      if (pn < 0) return -1;
      for (int c = 0; c < scan.n_cols; ++c) {
        const NsType want = static_cast<NsType>(scan.col_types[c]);
        const int src = wanted[static_cast<size_t>(c)];
        if (src < 0) {
          if (want == NS_F64) {
            (*f64)[static_cast<size_t>(c)].resize(
                (*f64)[static_cast<size_t>(c)].size() + static_cast<size_t>(pn), 0);
          } else if (want == NS_BOOL) {
            (*b)[static_cast<size_t>(c)].resize(
                (*b)[static_cast<size_t>(c)].size() + static_cast<size_t>(pn), 0);
          } else {
            (*i64)[static_cast<size_t>(c)].resize(
                (*i64)[static_cast<size_t>(c)].size() + static_cast<size_t>(pn), 0);
          }
          continue;
        }
        int table_c = -1;
        for (int k = 0; k < static_cast<int>(read_idx.size()); ++k) {
          if (read_idx[static_cast<size_t>(k)] == src) {
            table_c = k;
            break;
          }
        }
        if (want == NS_F64) {
          std::vector<double> cf(static_cast<size_t>(pn), 0);
          if (table_c >= 0 && table_c < table->num_columns()) {
            auto chunked = table->column(table_c);
            int off = 0;
            if (chunked) {
              for (int ch = 0; ch < chunked->num_chunks(); ++ch) {
                auto chunk = chunked->chunk(ch);
                if (!chunk) continue;
                std::vector<int64_t> ti;
                std::vector<double> tf;
                std::vector<uint8_t> tb;
                fill_from_array(*chunk, want, &ti, &tf, &tb);
                const int n = static_cast<int>(tf.size());
                for (int r = 0; r < n && off + r < pn; ++r) {
                  cf[static_cast<size_t>(off + r)] = tf[static_cast<size_t>(r)];
                }
                off += n;
              }
            }
          }
          (*f64)[static_cast<size_t>(c)].insert((*f64)[static_cast<size_t>(c)].end(),
                                                cf.begin(), cf.end());
        } else if (want == NS_BOOL) {
          std::vector<uint8_t> cb(static_cast<size_t>(pn), 0);
          if (table_c >= 0 && table_c < table->num_columns()) {
            auto chunked = table->column(table_c);
            int off = 0;
            if (chunked) {
              for (int ch = 0; ch < chunked->num_chunks(); ++ch) {
                auto chunk = chunked->chunk(ch);
                if (!chunk) continue;
                std::vector<int64_t> ti;
                std::vector<double> tf;
                std::vector<uint8_t> tb;
                fill_from_array(*chunk, want, &ti, &tf, &tb);
                const int n = static_cast<int>(tb.size());
                for (int r = 0; r < n && off + r < pn; ++r) {
                  cb[static_cast<size_t>(off + r)] = tb[static_cast<size_t>(r)];
                }
                off += n;
              }
            }
          }
          (*b)[static_cast<size_t>(c)].insert((*b)[static_cast<size_t>(c)].end(),
                                              cb.begin(), cb.end());
        } else {
          std::vector<int64_t> ci(static_cast<size_t>(pn), 0);
          if (table_c >= 0 && table_c < table->num_columns()) {
            auto chunked = table->column(table_c);
            int off = 0;
            if (chunked) {
              for (int ch = 0; ch < chunked->num_chunks(); ++ch) {
                auto chunk = chunked->chunk(ch);
                if (!chunk) continue;
                std::vector<int64_t> ti;
                std::vector<double> tf;
                std::vector<uint8_t> tb;
                fill_from_array(*chunk, want, &ti, &tf, &tb);
                const int n = static_cast<int>(ti.size());
                for (int r = 0; r < n && off + r < pn; ++r) {
                  ci[static_cast<size_t>(off + r)] = ti[static_cast<size_t>(r)];
                }
                off += n;
              }
            }
          }
          (*i64)[static_cast<size_t>(c)].insert((*i64)[static_cast<size_t>(c)].end(),
                                                ci.begin(), ci.end());
        }
      }
      total += pn;
    }
    *nrows = total;
    std::fprintf(stderr, "nativesql: pq ok rows=%d rgs=%zu\n", total, rgs.size());
    std::fflush(stderr);
    return 0;
  } catch (...) {
    std::fprintf(stderr, "nativesql: pq exception path=%s\n",
                 sp.path ? sp.path : "null");
    std::fflush(stderr);
    return -1;
  }
}

} // namespace

extern "C" {

void ns_strdict_clear(void) {
  g_strdict.clear();
  g_seen.clear();
}

int ns_strdict_size(void) { return static_cast<int>(g_strdict.size()); }

int ns_strdict_at(int i, int64_t *hash, const char **utf8, int32_t *len) {
  if (i < 0 || i >= static_cast<int>(g_strdict.size())) return -1;
  if (hash) *hash = g_strdict[static_cast<size_t>(i)].first;
  if (utf8) *utf8 = g_strdict[static_cast<size_t>(i)].second.c_str();
  if (len) *len = static_cast<int32_t>(g_strdict[static_cast<size_t>(i)].second.size());
  return 0;
}

int ns_parquet_read(const NsFileScan *scan, NsBatch *out) {
  try {
  if (!scan || !out) return -1;
  std::memset(out, 0, sizeof(*out));
  out->n_cols = scan->n_cols;
  if (scan->n_cols <= 0) return 0;
  out->cols = static_cast<NsCol *>(std::calloc(static_cast<size_t>(scan->n_cols), sizeof(NsCol)));
  int total = 0;
  std::vector<std::vector<int64_t>> all_i(static_cast<size_t>(scan->n_cols));
  std::vector<std::vector<double>> all_f(static_cast<size_t>(scan->n_cols));
  std::vector<std::vector<uint8_t>> all_b(static_cast<size_t>(scan->n_cols));
  for (int s = 0; s < scan->n_splits; ++s) {
    std::vector<std::vector<int64_t>> i64;
    std::vector<std::vector<double>> f64;
    std::vector<std::vector<uint8_t>> b;
    int n = 0;
    if (read_one(scan->splits[s], *scan, &i64, &f64, &b, &n) != 0) {
      ns_batch_free(out);
      return -1;
    }
    if (static_cast<int>(i64.size()) != scan->n_cols) {
      ns_batch_free(out);
      return -1;
    }
    for (int c = 0; c < scan->n_cols; ++c) {
      all_i[static_cast<size_t>(c)].insert(all_i[static_cast<size_t>(c)].end(),
                                           i64[static_cast<size_t>(c)].begin(),
                                           i64[static_cast<size_t>(c)].end());
      all_f[static_cast<size_t>(c)].insert(all_f[static_cast<size_t>(c)].end(),
                                           f64[static_cast<size_t>(c)].begin(),
                                           f64[static_cast<size_t>(c)].end());
      all_b[static_cast<size_t>(c)].insert(all_b[static_cast<size_t>(c)].end(),
                                           b[static_cast<size_t>(c)].begin(),
                                           b[static_cast<size_t>(c)].end());
    }
    total += n;
  }
  out->n_rows = total;
  for (int c = 0; c < scan->n_cols; ++c) {
    NsType t = static_cast<NsType>(scan->col_types[c]);
    if (t == NS_F64) {
      append_col(out, c, NS_F64, all_f[static_cast<size_t>(c)].data(), total, 0);
    } else if (t == NS_BOOL) {
      append_col(out, c, NS_BOOL, all_b[static_cast<size_t>(c)].data(), total, 0);
    } else {
      append_col(out, c, NS_I64, all_i[static_cast<size_t>(c)].data(), total, 0);
    }
  }
  return 0;
  } catch (...) {
    ns_batch_free(out);
    return -1;
  }
}

void ns_parquet_set_hdfs_io(NsHdfsSizeFn size_fn, NsHdfsPreadFn pread_fn) {
  g_hdfs_size = size_fn;
  g_hdfs_pread = pread_fn;
}

} // extern C
