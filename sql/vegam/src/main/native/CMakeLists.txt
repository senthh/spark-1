# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)
project(vegam LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(JNI REQUIRED)

set(VEGAM_SRC vegam_jni.cpp vegam_engine.cpp vegam_pipeline.cpp)

if(DEFINED ENV{VELOX_HOME})
  add_compile_definitions(VEGAM_HAS_VELOX)
  list(APPEND VEGAM_SRC vegam_velox.cpp vegam_hadoop_file.cpp)
  message(STATUS "Vegam: linking Velox from $ENV{VELOX_HOME}")
endif()

add_library(vegam SHARED ${VEGAM_SRC})
target_include_directories(vegam PRIVATE ${JNI_INCLUDE_DIRS})
if(DEFINED ENV{VELOX_HOME})
  set(VELOX_HOME $ENV{VELOX_HOME})
  set(VELOX_BUILD "${VELOX_HOME}/_build")
  if(DEFINED ENV{VELOX_LIB})
    set(VELOX_LIB $ENV{VELOX_LIB})
  elseif(EXISTS "${VELOX_BUILD}/lib/libvelox.so")
    set(VELOX_LIB "${VELOX_BUILD}/lib/libvelox.so")
  elseif(EXISTS "${VELOX_BUILD}/libvelox.so")
    set(VELOX_LIB "${VELOX_BUILD}/libvelox.so")
  else()
    set(VELOX_LIB velox)
  endif()
  message(STATUS "Vegam: VELOX_LIB=${VELOX_LIB}")

  set(VELOX_INCS
      ${VELOX_HOME}
      ${VELOX_HOME}/velox
      ${VELOX_BUILD}
      ${VELOX_BUILD}/_deps/folly-src
      ${VELOX_BUILD}/_deps/folly-build
      ${VELOX_BUILD}/_deps/fmt-src/include
      ${VELOX_BUILD}/_deps/glog-build
      ${VELOX_BUILD}/_deps/gflags-build/include
      ${VELOX_BUILD}/_deps/xsimd-src/include
      ${VELOX_BUILD}/_deps/re2-src
      ${VELOX_BUILD}/_deps/simdjson-src/include
      ${VELOX_BUILD}/_deps/fastfloat-src/include
      ${VELOX_HOME}/velox/external/xxhash)
  if(EXISTS "${VELOX_BUILD}/compile_commands.json")
    execute_process(
      COMMAND
        python3 -c
        "import json,re,sys; d=json.load(open(sys.argv[1]));\
cmd=next((e['command'] for e in d if e.get('file','').endswith('/Task.cpp')), '');\
incs=[];\
[incs.append(x) for x in re.findall(r'(?:-I|-isystem )(\\S+)', cmd) if x not in incs];\
print(';'.join(incs))"
        ${VELOX_BUILD}/compile_commands.json
      OUTPUT_VARIABLE VELOX_CC_INCS
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(VELOX_CC_INCS)
      list(APPEND VELOX_INCS ${VELOX_CC_INCS})
    endif()
  endif()
  target_include_directories(vegam PRIVATE ${VELOX_INCS})
  target_compile_definitions(
    vegam
    PRIVATE
      FOLLY_HAS_COROUTINES=1
      GFLAGS_IS_A_DLL=0
      GLOG_CUSTOM_PREFIX_SUPPORT)
  # System Arrow 15 + Parquet (shared). Avoid find_package(Arrow): it
  # prefers /usr/local/lib64/libarrow.a and then fails looking up Thrift.
  set(ARROW_LIB /usr/lib64/libarrow.so)
  set(PARQUET_LIB /usr/lib64/libparquet.so)
  if(NOT EXISTS "${ARROW_LIB}" OR NOT EXISTS "${PARQUET_LIB}")
    message(FATAL_ERROR "Vegam: need ${ARROW_LIB} and ${PARQUET_LIB}")
  endif()
  target_include_directories(vegam PRIVATE /usr/include)
  message(STATUS "Vegam: Arrow/Parquet ${ARROW_LIB} ${PARQUET_LIB}")
  # JNI headers only. Do not link libjawt/libjvm; the JVM provides them.
  target_link_libraries(
    vegam
    PRIVATE
      ${VELOX_LIB}
      ${ARROW_LIB}
      ${PARQUET_LIB})
  set_target_properties(
    vegam
    PROPERTIES
      BUILD_RPATH_USE_ORIGIN ON
      INSTALL_RPATH "$ORIGIN"
      BUILD_RPATH "$ORIGIN;${VELOX_BUILD}/lib;${VELOX_BUILD}/_deps/folly-build;${VELOX_BUILD}/_deps/glog-build;${VELOX_BUILD}/_deps/icu/lib")
else()
  target_link_libraries(vegam PRIVATE)
endif()
