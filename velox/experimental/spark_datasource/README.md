# Spark native data source (PoC)

A proof of concept of a Spark native data source
([apache/spark#59313](https://github.com/apache/spark/pull/59313)) implemented with Velox. Spark
reads Parquet files with Velox's Parquet reader and gets the data as Arrow batches through the
Arrow C data interface, without any JVM code for the data source.

```python
spark.read.format("velox").option("path", "/data/table").load()
```

| Option | Meaning |
|---|---|
| `path` | A Parquet file, or a directory of Parquet files (required) |
| `batch_size` | The number of rows of each Arrow batch, 4096 by default |

## Scope of the first version

- Batch reads only, without pushdown: no predicates, no column pruning, no limit. Spark
  evaluates filters and projections itself.
- Only integer (`tinyint`, `smallint`, `int`, `bigint`) and string columns. A file with another
  column type fails with "only integer and string columns are supported".
- Each file is one partition. Only local files.
- It implements the 8 JNI functions of `org.apache.spark.sql.datasource.NativeBridge` needed for
  batch reads: `abiVersion`, `createDataSource`, `closeDataSource`, `schema`, `createReader`,
  `partitions`, `read`, `closeReader`. The other functions are optional or belong to operations
  that Spark then reports as not supported (streaming reads, writes).

## How it works

| `NativeBridge` function | Where | Velox |
|---|---|---|
| `createDataSource` | driver | Keeps the options `path` and `batch_size` |
| `schema` | driver | Opens the first file with `parquet::ParquetReader` and exports its `RowType` with `exportToArrow` |
| `createReader` | driver | Lists the files of `path` |
| `partitions` | driver | One partition per file: `<batch_size>\n<path>` |
| `read` | executor | Returns an `ArrowArrayStream` over a `RowReader` of the file. Each `get_next` reads `batch_size` rows and exports them with `exportToArrow` |
| `closeDataSource`, `closeReader` | driver | Free the handles |

Without `serializeReader`, Spark passes an empty reader state to `read`, so a partition carries
everything an executor needs.

The Arrow export flattens dictionary and constant vectors, because Spark rejects dictionary
encoding. Strings use 32-bit offsets (`utf8`), Velox's default.

Ownership follows the `NativeBridge` contract: Spark allocates the `ArrowSchema` and
`ArrowArrayStream` structs, the library initializes them, and Spark releases them. The stream
owns its Parquet reader and is freed by its `release` callback. The exported arrays keep their
Velox vectors alive until Spark releases them. All the vectors are allocated from one process-wide
leaf memory pool.

C++ exceptions never cross JNI: the JNI functions turn them into a `java.lang.RuntimeException`,
which Spark reports as `NATIVE_DATA_SOURCE_ERROR`, and the stream returns `EIO` with the message
in `get_last_error`.

`libspark_datasource_velox.so` links Velox and its dependencies statically, and exports only the
`Java_org_apache_spark_sql_datasource_NativeBridge_*` functions (`symbols.map`), so it can be
loaded in the same JVM as another copy of Velox, such as Gluten's `libvelox.so`.

## Build

The target is off by default. Enable it in an existing build of Velox, for example the one of
Gluten (`dev/builddeps-veloxbe.sh ... --enable_vcpkg=ON --velox_home=/velox`), with Gluten's vcpkg
environment:

```bash
source /opt/rh/gcc-toolset-12/enable
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk          # for jni.h
export VCPKG_ROOT=$GLUTEN_HOME/dev/vcpkg/.vcpkg VCPKG_TRIPLET=x64-linux-avx \
       VCPKG_MANIFEST_DIR=$GLUTEN_HOME/dev/vcpkg
cd /velox/_build/release
cmake -DVELOX_BUILD_SPARK_DATASOURCE=ON .
ninja spark_datasource_velox
# -> velox/experimental/spark_datasource/libspark_datasource_velox.so
```

## Run

Spark finds the library from its file name: the data source `velox` is
`libspark_datasource_velox.so`, in a directory of the native library path, such as
`spark.driver.extraLibraryPath` and `spark.executor.extraLibraryPath`. It needs a Spark build
that contains apache/spark#59313.

```bash
export SPARK_HOME=/spark   # apache/spark with PR #59313, built with build/sbt -Phive package
export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*.zip)
LIB_DIR=/velox/_build/release/velox/experimental/spark_datasource \
  python3 velox/experimental/spark_datasource/test_velox_datasource.py
```

`test_velox_datasource.py` writes Parquet files with Spark, reads them with `velox`, and compares
the schema, the number of partitions, counts, a filter, an aggregation, all rows and a SQL query
with Spark's own Parquet reader. It also checks that a `double` column is rejected.

## Example: read a file and look at it in the Spark history server

`example/velox_read_example.py` writes a small Parquet file (10 rows: `id` bigint, `qty` int,
`name` string with a null, `category` string), reads it with `velox`, and runs a `show`, a
`count` and a `GROUP BY` query. It writes the event log of the application as one uncompressed
file (`spark.eventLog.rolling.enabled=false`).

1. Build the library and Spark as above, and create the directories:

   ```bash
   mkdir -p /opt/spark/work-dir /opt/spark/events
   ```

2. Run the example:

   ```bash
   export SPARK_HOME=/spark   # apache/spark with PR #59313
   export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*.zip)
   LIB_DIR=/velox/_build/release/velox/experimental/spark_datasource \
   WORK_DIR=/opt/spark/work-dir EVENT_DIR=/opt/spark/events \
     python3 velox/experimental/spark_datasource/example/velox_read_example.py
   ```

   The plan shows the scan of the data source, and the query returns:

   ```
   == Physical Plan ==
   *(1) ColumnarToRow
   +- BatchScan velox[id#0L, qty#1, name#2, category#3] velox RuntimeFilters: []

   +--------+---+---+--------+
   |category|  n|qty|max_name|
   +--------+---+---+--------+
   |   berry|  3| 24|   grape|
   |  citrus|  1| 10|   lemon|
   |   fruit|  3| 14|    kiwi|
   |   melon|  1|  4|honeydew|
   +--------+---+---+--------+
   event log: /opt/spark/events/local-<timestamp>
   ```

3. Start the history server of the same Spark build on the event log directory:

   ```bash
   SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///opt/spark/events" \
     $SPARK_HOME/sbin/start-history-server.sh
   ```

   It listens on port 18080. Open http://localhost:18080 (from another machine, forward the port,
   for example with `ssh -L 18080:localhost:18080 <host>`). The application
   `velox-native-datasource` has 4 SQL queries: the `show`, the `count` and the `GROUP BY` query
   have a `BatchScan velox` node, and the fourth one is the `CreateViewCommand` of
   `createOrReplaceTempView`. Check it from the command line with:

   ```bash
   curl -s http://localhost:18080/api/v1/applications
   ```

   Stop it with `$SPARK_HOME/sbin/stop-history-server.sh`.

Use the history server of the Spark build that wrote the event log: a release of Spark without
apache/spark#59313 can't run the data source, and may not read the event log of a newer version.
