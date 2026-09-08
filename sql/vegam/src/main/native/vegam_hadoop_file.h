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

#pragma once

#ifdef VEGAM_HAS_VELOX

#include <jni.h>
#include <string>

#include "velox/common/file/File.h"

namespace facebook::velox {

/**
 * Velox ReadFile that pulls byte ranges through HadoopBytes JNI.
 * No libhdfs. POSIX, hdfs://, and s3a:// use the same Java Hadoop client.
 */
class HadoopReadFile : public ReadFile {
 public:
  HadoopReadFile(JNIEnv* env, std::string path);
  ~HadoopReadFile() override;

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const FileIoContext& context = {}) const override;

  bool shouldCoalesce() const final {
    return true;
  }

  uint64_t size() const final {
    return size_;
  }

  uint64_t memoryUsage() const final {
    return 0;
  }

  std::string getName() const override {
    return path_;
  }

  uint64_t getNaturalReadSize() const override {
    return 1 << 20;
  }

 private:
  JNIEnv* env_;
  std::string path_;
  uint64_t size_;
};

} // namespace facebook::velox

#endif
