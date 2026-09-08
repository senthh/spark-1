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

#ifdef VEGAM_HAS_VELOX

#include "vegam_hadoop_file.h"

#include <cstring>

namespace facebook::velox {
namespace {

jclass hadoop_bytes(JNIEnv* env) {
  return env->FindClass("org/apache/spark/sql/vegam/exec/HadoopBytes");
}

}  // namespace

HadoopReadFile::HadoopReadFile(JNIEnv* env, std::string path)
    : env_(env), path_(std::move(path)), size_(0) {
  jclass cls = hadoop_bytes(env_);
  if (cls == nullptr) {
    return;
  }
  jmethodID mid = env_->GetStaticMethodID(cls, "size", "(Ljava/lang/String;)J");
  if (mid == nullptr) {
    env_->DeleteLocalRef(cls);
    return;
  }
  jstring jp = env_->NewStringUTF(path_.c_str());
  size_ = static_cast<uint64_t>(env_->CallStaticLongMethod(cls, mid, jp));
  env_->DeleteLocalRef(jp);
  env_->DeleteLocalRef(cls);
}

HadoopReadFile::~HadoopReadFile() {
  if (env_ == nullptr) {
    return;
  }
  jclass cls = hadoop_bytes(env_);
  if (cls == nullptr) {
    return;
  }
  jmethodID mid =
      env_->GetStaticMethodID(cls, "closePath", "(Ljava/lang/String;)V");
  if (mid != nullptr) {
    jstring jp = env_->NewStringUTF(path_.c_str());
    env_->CallStaticVoidMethod(cls, mid, jp);
    env_->DeleteLocalRef(jp);
  }
  env_->DeleteLocalRef(cls);
}

std::string_view HadoopReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buf,
    const FileIoContext&) const {
  bytesRead_ += length;
  if (length == 0 || buf == nullptr || env_ == nullptr) {
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  jclass cls = hadoop_bytes(env_);
  if (cls == nullptr) {
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  jmethodID mid =
      env_->GetStaticMethodID(cls, "pread", "(Ljava/lang/String;JI)[B");
  if (mid == nullptr) {
    env_->DeleteLocalRef(cls);
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  jstring jp = env_->NewStringUTF(path_.c_str());
  auto arr = reinterpret_cast<jbyteArray>(
      env_->CallStaticObjectMethod(
          cls, mid, jp, static_cast<jlong>(offset), static_cast<jint>(length)));
  env_->DeleteLocalRef(jp);
  env_->DeleteLocalRef(cls);
  if (arr == nullptr) {
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  jsize n = env_->GetArrayLength(arr);
  if (n > static_cast<jsize>(length)) {
    n = static_cast<jsize>(length);
  }
  env_->GetByteArrayRegion(arr, 0, n, reinterpret_cast<jbyte*>(buf));
  env_->DeleteLocalRef(arr);
  return std::string_view(static_cast<const char*>(buf), static_cast<size_t>(n));
}

}  // namespace facebook::velox

#endif
