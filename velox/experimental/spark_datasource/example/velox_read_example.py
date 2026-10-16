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

"""Writes a small Parquet file, reads it with the `velox` native data source, and writes the
event log of the application, for the Spark history server.

    LIB_DIR=<dir of libspark_datasource_velox.so> python3 velox_read_example.py

Environment variables:
    LIB_DIR    the directory of libspark_datasource_velox.so
    WORK_DIR   where to write small.parquet, /opt/spark/work-dir by default
    EVENT_DIR  the event log directory, /opt/spark/events by default
"""
import os

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession

lib_dir = os.environ["LIB_DIR"]
work_dir = os.environ.get("WORK_DIR", "/opt/spark/work-dir")
event_dir = os.environ.get("EVENT_DIR", "/opt/spark/events")

# Written with pyarrow, so that the event log only contains the reads of the data source.
path = os.path.join(work_dir, "small.parquet")
pq.write_table(pa.table({
    "id": pa.array(range(1, 11), pa.int64()),
    "qty": pa.array([5, 3, 8, 1, 9, 2, 7, 4, 6, 10], pa.int32()),
    "name": pa.array(["apple", "banana", "cherry", None, "elderberry", "fig", "grape",
                      "honeydew", "kiwi", "lemon"]),
    "category": pa.array(["fruit", "fruit", "berry", "berry", "berry", "fruit", "berry",
                          "melon", "fruit", "citrus"]),
}), path)

spark = (SparkSession.builder.master("local[2]")
         .appName("velox-native-datasource")
         .config("spark.driver.extraLibraryPath", lib_dir)
         .config("spark.executor.extraLibraryPath", lib_dir)
         .config("spark.eventLog.enabled", "true")
         .config("spark.eventLog.dir", "file://" + event_dir)
         .config("spark.eventLog.rolling.enabled", "false")
         .config("spark.eventLog.compress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("velox").option("path", path).load()
df.printSchema()
df.explain()
df.show()
print("count:", df.count())
df.createOrReplaceTempView("small")
spark.sql("SELECT category, COUNT(*) AS n, SUM(qty) AS qty, MAX(name) AS max_name "
          "FROM small WHERE qty > 2 GROUP BY category ORDER BY category").show()
print("event log:", os.path.join(event_dir, spark.sparkContext.applicationId))
spark.stop()
