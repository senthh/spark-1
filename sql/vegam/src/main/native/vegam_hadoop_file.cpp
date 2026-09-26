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
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"

namespace {

JavaVM* g_jvm = nullptr;

jclass hadoop_bytes(JNIEnv* env) {
  return env->FindClass("org/apache/spark/sql/vegam/exec/HadoopBytes");
}

bool hadoop_scheme(std::string_view path) {
  return path.rfind("hdfs://", 0) == 0 || path.rfind("viewfs://", 0) == 0 ||
      path.rfind("s3a://", 0) == 0 || path.rfind("file:", 0) == 0 ||
      (!path.empty() && path[0] == '/');
}

// Clears a pending Java exception and rethrows it as std::runtime_error so a
// failed Hadoop read fails the Velox task instead of looking like a short read.
void rethrow_java(JNIEnv* env, const std::string& what) {
  if (!env->ExceptionCheck()) {
    return;
  }
  jthrowable ex = env->ExceptionOccurred();
  env->ExceptionClear();
  std::string msg = "unknown";
  jclass cls = env->FindClass("java/lang/Throwable");
  jmethodID to_string = env->GetMethodID(cls, "toString", "()Ljava/lang/String;");
  auto js = reinterpret_cast<jstring>(env->CallObjectMethod(ex, to_string));
  if (!env->ExceptionCheck() && js != nullptr) {
    const char* c = env->GetStringUTFChars(js, nullptr);
    msg = c ? c : "unknown";
    env->ReleaseStringUTFChars(js, c);
  }
  env->ExceptionClear();
  throw std::runtime_error(what + ": " + msg);
}

}  // namespace

void vegam_set_javavm(JavaVM* vm) {
  g_jvm = vm;
}

JNIEnv* vegam_jni_env() {
  if (g_jvm == nullptr) {
    return nullptr;
  }
  JNIEnv* env = nullptr;
  jint rc = g_jvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
  if (rc == JNI_OK) {
    return env;
  }
  rc = g_jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
  return rc == JNI_OK ? env : nullptr;
}

namespace facebook::velox {

HadoopReadFile::HadoopReadFile(std::string path)
    : path_(std::move(path)), size_(0) {
  JNIEnv* env = vegam_jni_env();
  if (env == nullptr) {
    return;
  }
  jclass cls = hadoop_bytes(env);
  if (cls == nullptr) {
    return;
  }
  jmethodID mid = env->GetStaticMethodID(cls, "size", "(Ljava/lang/String;)J");
  if (mid == nullptr) {
    env->DeleteLocalRef(cls);
    return;
  }
  jstring jp = env->NewStringUTF(path_.c_str());
  size_ = static_cast<uint64_t>(env->CallStaticLongMethod(cls, mid, jp));
  env->DeleteLocalRef(jp);
  env->DeleteLocalRef(cls);
  rethrow_java(env, "HadoopBytes.size " + path_);
}

std::string_view HadoopReadFile::pread(
    uint64_t offset,
    uint64_t length,
    void* buf,
    const FileIoContext&) const {
  if (length == 0 || buf == nullptr) {
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  JNIEnv* env = vegam_jni_env();
  if (env == nullptr) {
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  jclass cls = hadoop_bytes(env);
  if (cls == nullptr) {
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  jmethodID mid =
      env->GetStaticMethodID(cls, "pread", "(Ljava/lang/String;JI)[B");
  if (mid == nullptr) {
    env->DeleteLocalRef(cls);
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  jstring jp = env->NewStringUTF(path_.c_str());
  auto arr = reinterpret_cast<jbyteArray>(
      env->CallStaticObjectMethod(
          cls, mid, jp, static_cast<jlong>(offset), static_cast<jint>(length)));
  env->DeleteLocalRef(jp);
  env->DeleteLocalRef(cls);
  rethrow_java(env, "HadoopBytes.pread " + path_);
  if (arr == nullptr) {
    return std::string_view(static_cast<const char*>(buf), 0);
  }
  jsize n = env->GetArrayLength(arr);
  if (n > static_cast<jsize>(length)) {
    n = static_cast<jsize>(length);
  }
  env->GetByteArrayRegion(arr, 0, n, reinterpret_cast<jbyte*>(buf));
  env->DeleteLocalRef(arr);
  return std::string_view(static_cast<const char*>(buf), static_cast<size_t>(n));
}

class HadoopFileSystem : public filesystems::FileSystem {
 public:
  explicit HadoopFileSystem(std::shared_ptr<const config::ConfigBase> config)
      : filesystems::FileSystem(std::move(config)) {}

  std::string name() const override {
    return "hadoop-bytes";
  }

  std::string_view extractPath(std::string_view path) const override {
    if (path.rfind("file://", 0) == 0) {
      return path.substr(7);
    }
    if (path.rfind("file:", 0) == 0) {
      return path.substr(5);
    }
    return path;
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const filesystems::FileOptions&) override {
    return std::make_unique<HadoopReadFile>(std::string(extractPath(path)));
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view,
      const filesystems::FileOptions&) override {
    throw std::runtime_error("vegam hadoop fs is read-only");
  }

  void remove(std::string_view) override {}

  void rename(std::string_view, std::string_view, bool) override {}

  bool exists(std::string_view) override {
    return true;
  }

  std::vector<std::string> list(std::string_view) override {
    return {};
  }

  void mkdir(std::string_view, const filesystems::DirectoryOptions&) override {}

  void rmdir(std::string_view) override {}
};

}  // namespace facebook::velox

void vegam_register_hadoop_fs() {
  static std::once_flag once;
  std::call_once(once, [] {
    facebook::velox::filesystems::registerFileSystem(
        [](std::string_view path) { return hadoop_scheme(path); },
        [](std::shared_ptr<const facebook::velox::config::ConfigBase> cfg,
           std::string_view) {
          return std::make_shared<facebook::velox::HadoopFileSystem>(
              std::move(cfg));
        });
  });
}

#endif
