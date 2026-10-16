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

"""Reads Parquet files written by Spark with the `velox` native data source, and compares the
result with Spark's own Parquet reader.

    LIB_DIR=<dir of libspark_datasource_velox.so> python3 test_velox_datasource.py
"""
import os
import shutil
import sys
import tempfile

from pyspark.sql import SparkSession

lib_dir = os.environ.get("LIB_DIR", os.path.join(os.path.dirname(__file__), "build"))
spark = (SparkSession.builder.master("local[2]")
         .config("spark.driver.extraLibraryPath", lib_dir)
         .config("spark.executor.extraLibraryPath", lib_dir)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

path = tempfile.mkdtemp(prefix="velox_ds_")
shutil.rmtree(path)
spark.sql("""
  SELECT id,
         CAST(id % 7 AS INT) AS small,
         IF(id % 5 = 0, NULL, CONCAT('name_', id)) AS name,
         IF(id % 2 = 0, 'even', 'odd') AS parity
  FROM range(0, 100000, 1, 4)
""").write.parquet(path)

expected = spark.read.parquet(path)
actual = spark.read.format("velox").option("path", path).option("batch_size", "1000").load()

failures = 0


def check(name, got, want):
    global failures
    ok = got == want
    failures += not ok
    print(f"{'OK  ' if ok else 'FAIL'} {name}: {got if ok else f'{got} != {want}'}", flush=True)


print(actual.schema.simpleString())
actual.explain()
actual.show(5)
check("schema", actual.schema.simpleString(), expected.schema.simpleString())
check("partitions", actual.rdd.getNumPartitions(), len([f for f in os.listdir(path)
                                                        if f.endswith(".parquet")]))
check("count", actual.count(), 100000)
check("filter + count", actual.filter("parity = 'even' AND name IS NOT NULL").count(),
      expected.filter("parity = 'even' AND name IS NOT NULL").count())
check("aggregate", actual.groupBy("small").agg({"id": "sum", "name": "count"})
      .orderBy("small").collect(),
      expected.groupBy("small").agg({"id": "sum", "name": "count"}).orderBy("small").collect())
check("rows (exceptAll both ways)",
      (actual.exceptAll(expected).count(), expected.exceptAll(actual).count()), (0, 0))

actual.createOrReplaceTempView("t")
expected.createOrReplaceTempView("e")
query = "SELECT parity, COUNT(DISTINCT name), MAX(id) FROM {} GROUP BY parity ORDER BY parity"
check("SQL", spark.sql(query.format("t")).collect(), spark.sql(query.format("e")).collect())

# Other types are rejected with a clear error.
spark.range(3).selectExpr("id", "id * 1.5D AS value").write.parquet(path + "_double")
try:
    spark.read.format("velox").option("path", path + "_double").load().collect()
    check("double rejected", "no error", "error")
except Exception as e:
    check("double rejected", "only integer and string columns are supported" in str(e), True)

shutil.rmtree(path)
shutil.rmtree(path + "_double")
spark.stop()
sys.exit(1 if failures else 0)
