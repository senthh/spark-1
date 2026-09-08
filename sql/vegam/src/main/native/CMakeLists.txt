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

set(VEGAM_SRC vegam_jni.cpp vegam_engine.cpp)

if(DEFINED ENV{VELOX_HOME})
  add_compile_definitions(VEGAM_HAS_VELOX)
  list(APPEND VEGAM_SRC vegam_velox.cpp vegam_hadoop_file.cpp)
  message(STATUS "Vegam: linking Velox from $ENV{VELOX_HOME}")
endif()

add_library(vegam SHARED ${VEGAM_SRC})
target_include_directories(vegam PRIVATE ${JNI_INCLUDE_DIRS})
if(DEFINED ENV{VELOX_HOME})
  set(VELOX_HOME $ENV{VELOX_HOME})
  target_include_directories(
    vegam
    PRIVATE
      ${VELOX_HOME}
      ${VELOX_HOME}/velox
      ${VELOX_HOME}/_build
      ${VELOX_HOME}/_build/velox)
  if(DEFINED ENV{VELOX_LIB})
    set(VELOX_LIB $ENV{VELOX_LIB})
  elseif(EXISTS "${VELOX_HOME}/_build/libvelox.so")
    set(VELOX_LIB "${VELOX_HOME}/_build/libvelox.so")
  else()
    set(VELOX_LIB velox)
  endif()
  target_link_libraries(vegam PRIVATE ${JNI_LIBRARIES} ${VELOX_LIB})
  set_target_properties(vegam PROPERTIES BUILD_RPATH_USE_ORIGIN ON INSTALL_RPATH "$ORIGIN")
else()
  target_link_libraries(vegam PRIVATE ${JNI_LIBRARIES})
endif()
