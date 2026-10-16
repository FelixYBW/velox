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

# libspark_datasource_velox.so: the Spark native data source "velox" (apache/spark#59313), which
# reads Parquet files with Velox. Velox and its dependencies are linked statically and hidden, so
# that the library can be loaded next to another copy of Velox, such as Gluten's.

if(NOT DEFINED ENV{JAVA_HOME})
  message(FATAL_ERROR "Set JAVA_HOME to a JDK to build the Spark native data source")
endif()

add_library(spark_datasource_velox SHARED SparkNativeDataSource.cpp)

target_include_directories(
  spark_datasource_velox
  PRIVATE $ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/linux
)

if(VELOX_MONO_LIBRARY)
  target_link_libraries(spark_datasource_velox PRIVATE velox)
else()
  target_link_libraries(
    spark_datasource_velox
    PRIVATE velox_dwio_parquet_reader velox_arrow_bridge velox_memory velox_file
  )
endif()

set_target_properties(
  spark_datasource_velox
  PROPERTIES CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN ON
)

target_link_options(
  spark_datasource_velox
  PRIVATE
    -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map
    -Wl,--exclude-libs,ALL
)
