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

#ifndef VEGAM_SCHEDULER_H
#define VEGAM_SCHEDULER_H

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

// HyPer-style morsels inside one Spark task. 10K rows is the paper default.
// Workers pop from a shared deque; a late worker steals the remaining tail.
namespace vegam {

constexpr int kMorselRows = 10000;

struct Morsel {
  int start = 0;
  int len = 0;
};

inline std::vector<Morsel> split_morsels(int num_rows, int morsel_rows = kMorselRows) {
  std::vector<Morsel> out;
  if (num_rows <= 0) {
    return out;
  }
  int size = morsel_rows > 0 ? morsel_rows : kMorselRows;
  for (int i = 0; i < num_rows; i += size) {
    int n = num_rows - i;
    if (n > size) {
      n = size;
    }
    out.push_back(Morsel{i, n});
  }
  return out;
}

class StealQueue {
 public:
  explicit StealQueue(std::vector<Morsel> morsels) : morsels_(std::move(morsels)) {}

  bool steal(Morsel* out) {
    std::lock_guard<std::mutex> g(mu_);
    if (next_ >= static_cast<int>(morsels_.size())) {
      return false;
    }
    *out = morsels_[next_++];
    stolen_++;
    return true;
  }

  int stolen() const { return stolen_.load(); }

 private:
  std::vector<Morsel> morsels_;
  int next_ = 0;
  std::mutex mu_;
  std::atomic<int> stolen_{0};
};

inline int run_morsels(int num_rows, int threads, const std::function<void(Morsel)>& fn) {
  auto morsels = split_morsels(num_rows);
  StealQueue q(std::move(morsels));
  int n = threads > 0 ? threads : 1;
  if (n > 1 && num_rows > kMorselRows) {
    std::vector<std::thread> pool;
    pool.reserve(static_cast<size_t>(n));
    for (int t = 0; t < n; t++) {
      pool.emplace_back([&q, &fn]() {
        Morsel m;
        while (q.steal(&m)) {
          fn(m);
        }
      });
    }
    for (auto& th : pool) {
      th.join();
    }
  } else {
    Morsel m;
    while (q.steal(&m)) {
      fn(m);
    }
  }
  return q.stolen();
}

}  // namespace vegam

#endif
