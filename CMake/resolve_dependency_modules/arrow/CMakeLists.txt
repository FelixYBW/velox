# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
project(Arrow)

if(VELOX_ENABLE_ARROW)
  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep")
  set(ARROW_CMAKE_ARGS
      -DARROW_PARQUET=ON
      -DARROW_FILESYSTEM=ON
      -DARROW_PROTOBUF_USE_SHARED=OFF
      -DARROW_DEPENDENCY_USE_SHARED=OFF
      -DARROW_DEPENDENCY_SOURCE=BUNDLED
      -DARROW_WITH_THRIFT=ON
      -DARROW_WITH_LZ4=ON
      -DARROW_WITH_SNAPPY=ON
      -DARROW_WITH_ZLIB=ON
      -DARROW_WITH_ZSTD=ON
      -DARROW_JEMALLOC=OFF
      -DARROW_SIMD_LEVEL=NONE
      -DARROW_RUNTIME_SIMD_LEVEL=NONE
      -DARROW_WITH_UTF8PROC=OFF
      -DARROW_TESTING=ON
      -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}/install
      -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
      -DARROW_BUILD_STATIC=ON
      -DARROW_BUILD_SHARED=OFF
      -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH})
  set(ARROW_LIBDIR ${ARROW_PREFIX}/install/${CMAKE_INSTALL_LIBDIR})

  add_library(thrift STATIC IMPORTED GLOBAL)
  set(THRIFT_ROOT ${ARROW_PREFIX}/src/arrow_ep-build/thrift_ep-install)
  set(THRIFT_LIB ${THRIFT_ROOT}/lib/libthrift.a)

  file(MAKE_DIRECTORY ${THRIFT_ROOT}/include)
  set(THRIFT_INCLUDE_DIR ${THRIFT_ROOT}/include)

  set_property(TARGET thrift PROPERTY INTERFACE_INCLUDE_DIRECTORIES
                                      ${THRIFT_INCLUDE_DIR})
  set_property(TARGET thrift PROPERTY IMPORTED_LOCATION ${THRIFT_LIB})

  set(VELOX_ARROW_BUILD_VERSION 15.0.0)
  set(VELOX_ARROW_BUILD_SHA256_CHECKSUM
      01dd3f70e85d9b5b933ec92c0db8a4ef504a5105f78d2d8622e84279fb45c25d)
  set(VELOX_ARROW_SOURCE_URL
      "https://archive.apache.org/dist/arrow/arrow-${VELOX_ARROW_BUILD_VERSION}/apache-arrow-${VELOX_ARROW_BUILD_VERSION}.tar.gz"
  )

  resolve_dependency_url(ARROW)

  ExternalProject_Add(
    arrow_ep
    PREFIX ${ARROW_PREFIX}
    URL ${VELOX_ARROW_SOURCE_URL}
    PATCH_COMMAND patch -p1 < ${CMAKE_CURRENT_SOURCE_DIR}/modify_arrow.patch
    COMMAND patch -p1 <
            ${CMAKE_CURRENT_SOURCE_DIR}/modify_arrow_dataset_scan_option.patch
    URL_HASH ${VELOX_ARROW_BUILD_SHA256_CHECKSUM}
    SOURCE_SUBDIR cpp
    CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    BUILD_BYPRODUCTS ${ARROW_LIBDIR}/libarrow.a ${ARROW_LIBDIR}/libparquet.a
                     ${ARROW_LIBDIR}/libarrow_testing.a ${THRIFT_LIB})

  add_library(arrow STATIC IMPORTED GLOBAL)
  add_library(arrow_testing STATIC IMPORTED GLOBAL)
  add_library(parquet STATIC IMPORTED GLOBAL)
  add_dependencies(arrow arrow_ep)
  add_dependencies(arrow_testing arrow)
  add_dependencies(parquet arrow)
  file(MAKE_DIRECTORY ${ARROW_PREFIX}/install/include)
  set_target_properties(
    arrow arrow_testing parquet PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                           ${ARROW_PREFIX}/install/include)
  set_target_properties(arrow PROPERTIES IMPORTED_LOCATION
                                         ${ARROW_LIBDIR}/libarrow.a)
  set_property(TARGET arrow PROPERTY INTERFACE_LINK_LIBRARIES ${RE2} thrift)
  set_target_properties(
    arrow_testing PROPERTIES IMPORTED_LOCATION
                             ${ARROW_LIBDIR}/libarrow_testing.a)
  set_target_properties(parquet PROPERTIES IMPORTED_LOCATION
                                           ${ARROW_LIBDIR}/libparquet.a)

endif()
