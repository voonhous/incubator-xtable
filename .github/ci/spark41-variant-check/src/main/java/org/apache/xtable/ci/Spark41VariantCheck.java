/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.xtable.ci;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * Writes unshredded variant columns through the Hudi Spark 4.1 datasource with the Iceberg
 * pluggable table format active (bulk insert, upsert, delete on a copy-on-write table), then checks
 * what Spark 4.1 reads through the Iceberg 4.1 runtime: a format-version 3 table with real variant
 * columns, VARIANT-annotated data files, exactly the live rows, and the same values Hudi returns.
 *
 * <p>Every check runs even when an earlier one fails; the process exits with status 1 if any
 * failed, so the CI step fails with the full list.
 *
 * <p>This is a CI-only check, outside the XTable build: XTable has no Spark 4 build yet, so its pom
 * puts the table format jars (built for Scala 2.12 against the same Hudi commit) on the classpath
 * without their transitive dependencies, which Spark 4.1 and the Hudi Spark 4.1 bundle provide.
 *
 * <p>Usage: {@code Spark41VariantCheck <table path>}
 */
public class Spark41VariantCheck {

  private final List<String> failures = new ArrayList<>();
  private int checks;

  public static void main(String[] args) {
    Spark41VariantCheck check = new Spark41VariantCheck();
    try {
      check.run(args[0]);
    } catch (Throwable t) {
      t.printStackTrace(System.out);
      check.failures.add("unexpected exception: " + t);
    }
    System.out.println();
    if (check.failures.isEmpty()) {
      System.out.println("PASSED: all " + check.checks + " checks");
      System.exit(0);
    }
    System.out.println("FAILED: " + check.failures.size() + " of " + check.checks + " checks");
    check.failures.forEach(failure -> System.out.println("  - " + failure));
    System.exit(1);
  }

  private void run(String path) throws Exception {
    deleteDirectory(path);
    SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("spark41-variant-check")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            // path-based Iceberg reads go through this catalog; without it Iceberg registers a
            // Hive catalog, which needs the Hive metastore client on the classpath
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config("spark.sql.catalog.default_iceberg.warehouse", path)
            // the pluggable table format reads its settings from the Hadoop configuration
            .config("spark.hadoop.xtable.iceberg.format-version", "3")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    System.out.println("Spark " + spark.version() + ", table " + path);

    // bulk insert goes through Hudi's Spark row writer, the upsert and delete through the merge
    // path, so both writers' files end up in the table
    write(
        rows(
            spark,
            "('k1', 1L, 1, '{\"a\":1,\"b\":\"x\"}'),"
                + "('k2', 1L, 2, '[1,2,3]'),"
                + "('k3', 1L, 3, '{\"a\":{\"nested\":true},\"d\":1.5}'),"
                + "('k4', 1L, 4, '\"plain string\"'),"
                + "('k5', 1L, 5, 'null'),"
                + "('k6', 1L, 6, cast(null as string)),"
                + "('k7', 1L, 7, '12.34'),"
                + "('k8', 1L, 8, '9007199254740993')"),
        path,
        "bulk_insert");
    write(
        rows(
            spark,
            "('k1', 2L, 1, '{\"a\":11,\"b\":\"y\",\"c\":false}'),"
                + "('k2', 2L, 2, '2.5'),"
                + "('k9', 2L, 9, '\"2024-01-02\"')"),
        path,
        "upsert");
    write(rows(spark, "('k3', 3L, 3, 'null')"), path, "delete");

    Map<String, String> expectedJson = new LinkedHashMap<>();
    expectedJson.put("k1", "{\"a\":11,\"b\":\"y\",\"c\":false}");
    expectedJson.put("k2", "2.5");
    expectedJson.put("k4", "\"plain string\"");
    expectedJson.put("k5", "null");
    expectedJson.put("k6", null);
    expectedJson.put("k7", "12.34");
    expectedJson.put("k8", "9007199254740993");
    expectedJson.put("k9", "\"2024-01-02\"");

    checkIcebergMetadata(path);

    Dataset<Row> iceberg = spark.read().format("iceberg").load(path);
    check(
        iceberg.schema().apply("v").dataType().equals(DataTypes.VariantType),
        "Spark reads Iceberg column v as variant, got "
            + iceberg.schema().apply("v").dataType().catalogString());
    check(
        iceberg.schema().apply("v_req").dataType().equals(DataTypes.VariantType),
        "Spark reads Iceberg column v_req as variant, got "
            + iceberg.schema().apply("v_req").dataType().catalogString());
    iceberg.createOrReplaceTempView("ice");
    spark.read().format("hudi").load(path).createOrReplaceTempView("hudi");

    Map<String, String> icebergJson = jsonByKey(spark, "ice");
    Map<String, String> hudiJson = jsonByKey(spark, "hudi");
    long icebergRows = spark.sql("select count(*) from ice").first().getLong(0);
    System.out.println("Iceberg rows: " + icebergJson);
    System.out.println("Hudi rows:    " + hudiJson);
    check(
        icebergRows == expectedJson.size(),
        "Iceberg returns " + icebergRows + " rows, expected " + expectedJson.size()
            + " (a replaced or deleted data file still listed in the snapshot shows up here)");
    check(
        icebergJson.keySet().equals(expectedJson.keySet()),
        "Iceberg keys " + icebergJson.keySet() + ", expected " + expectedJson.keySet());
    check(hudiJson.equals(new TreeMap<>(expectedJson)), "Hudi read " + hudiJson);
    for (Map.Entry<String, String> e : expectedJson.entrySet()) {
      check(
          Objects.equals(icebergJson.get(e.getKey()), e.getValue()),
          "Iceberg to_json(v) of " + e.getKey() + " is " + icebergJson.get(e.getKey())
              + ", expected " + e.getValue());
    }
    checkTypedValues(spark);
    spark.stop();
  }

  private void checkIcebergMetadata(String path) throws Exception {
    Table table = new HadoopTables(new Configuration()).load(path);
    int formatVersion = ((BaseTable) table).operations().current().formatVersion();
    check(formatVersion == 3, "Iceberg format version is " + formatVersion + ", expected 3");
    check(
        Types.VariantType.get().equals(table.schema().findType("v")),
        "Iceberg type of v is " + table.schema().findType("v"));
    check(
        Types.VariantType.get().equals(table.schema().findType("v_req")),
        "Iceberg type of v_req is " + table.schema().findType("v_req"));

    Configuration conf = new Configuration();
    int files = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        files++;
        try (ParquetFileReader reader =
            ParquetFileReader.open(
                HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(task.file().location()), conf))) {
          MessageType schema = reader.getFooter().getFileMetaData().getSchema();
          for (String column : new String[] {"v", "v_req"}) {
            LogicalTypeAnnotation annotation = schema.getType(column).getLogicalTypeAnnotation();
            check(
                annotation != null && annotation.toString().startsWith("VARIANT"),
                "column " + column + " of " + task.file().location()
                    + " carries annotation " + annotation + ", expected VARIANT");
          }
        }
      }
    }
    check(files > 0, "the Iceberg snapshot lists no data files");
  }

  /**
   * Values read through Iceberg, extracted with Spark's variant functions and compared as typed
   * values. try_variant_get keeps a stray row of another shape from aborting the remaining checks;
   * such rows already fail the row-count and key checks.
   */
  private void checkTypedValues(SparkSession spark) {
    Row row =
        spark
            .sql(
                "select"
                    + " max(case when key = 'k1' then try_variant_get(v, '$.a', 'int') end),"
                    + " max(case when key = 'k1' then try_variant_get(v, '$.b', 'string') end),"
                    + " max(case when key = 'k1' then try_variant_get(v, '$.c', 'boolean') end),"
                    + " max(case when key = 'k2' then try_variant_get(v, '$', 'double') end),"
                    + " max(case when key = 'k4' then try_variant_get(v, '$', 'string') end),"
                    + " max(case when key = 'k5' then is_variant_null(v) end),"
                    + " max(case when key = 'k6' then v is null end),"
                    + " max(case when key = 'k7' then try_variant_get(v, '$', 'decimal(4,2)') end),"
                    + " max(case when key = 'k8' then try_variant_get(v, '$', 'bigint') end),"
                    + " max(case when key = 'k9' then try_variant_get(v, '$', 'string') end),"
                    + " sum(case when try_variant_get(v_req, '$', 'int') = cast(substr(key, 2) as int)"
                    + " then 0 else 1 end)"
                    + " from ice")
            .first();
    check(Objects.equals(row.get(0), 11), "k1 v.a = " + row.get(0) + ", expected 11");
    check(Objects.equals(row.get(1), "y"), "k1 v.b = " + row.get(1) + ", expected y");
    check(Objects.equals(row.get(2), false), "k1 v.c = " + row.get(2) + ", expected false");
    check(Objects.equals(row.get(3), 2.5d), "k2 v = " + row.get(3) + ", expected 2.5");
    check(
        Objects.equals(row.get(4), "plain string"),
        "k4 v = " + row.get(4) + ", expected plain string");
    check(Objects.equals(row.get(5), true), "k5 v must be a variant null, got " + row.get(5));
    check(Objects.equals(row.get(6), true), "k6 v must be SQL null, got " + row.get(6));
    check(
        row.get(7) instanceof BigDecimal
            && ((BigDecimal) row.get(7)).compareTo(new BigDecimal("12.34")) == 0,
        "k7 v = " + row.get(7) + ", expected 12.34");
    check(
        Objects.equals(row.get(8), 9007199254740993L),
        "k8 v = " + row.get(8) + ", expected 9007199254740993");
    check(
        Objects.equals(row.get(9), "2024-01-02"), "k9 v = " + row.get(9) + ", expected 2024-01-02");
    check(
        row.get(10) != null && ((Number) row.get(10)).longValue() == 0,
        row.get(10) + " rows have a v_req that differs from their row number");
  }

  /**
   * to_json(v) per record key. A key that appears more than once gets its extra rows under {@code
   * key#2}, {@code key#3} and so on, so duplicates fail the key comparison and show in the output.
   */
  private static Map<String, String> jsonByKey(SparkSession spark, String view) {
    Map<String, String> result = new TreeMap<>();
    for (Row row :
        spark.sql("select key, to_json(v) from " + view + " order by key").collectAsList()) {
      String key = row.getString(0);
      String slot = key;
      for (int copy = 2; result.containsKey(slot); copy++) {
        slot = key + "#" + copy;
      }
      result.put(slot, row.getString(1));
    }
    return result;
  }

  private static Dataset<Row> rows(SparkSession spark, String values) {
    return spark.sql(
        "select key, ts, cast(n as string) as name, parse_json(js) as v,"
            + " parse_json(cast(n as string)) as v_req from values "
            + values
            + " as t(key, ts, n, js)");
  }

  private static void write(Dataset<Row> rows, String path, String operation) {
    DataFrameWriter<Row> writer =
        rows.write()
            .format("hudi")
            .option("hoodie.table.name", "spark41_variant_check")
            .option("hoodie.datasource.write.recordkey.field", "key")
            .option("hoodie.datasource.write.precombine.field", "ts")
            .option("hoodie.datasource.write.partitionpath.field", "")
            .option(
                "hoodie.datasource.write.keygenerator.class",
                "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
            .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
            .option("hoodie.datasource.write.operation", operation)
            .option("hoodie.table.format", "ICEBERG")
            .option("hoodie.table.version", "8")
            .option("hoodie.write.table.version", "8")
            .option("hoodie.write.auto.upgrade", "false")
            .option("hoodie.metadata.enable", "false")
            .option("hoodie.parquet.variant.shredding.schema.inference.enabled", "false")
            .option("hoodie.parquet.variant.write.shredding.enabled", "false");
    writer.mode(SaveMode.Append).save(path);
    System.out.println(operation + " committed");
  }

  private void check(boolean condition, String failure) {
    checks++;
    if (!condition) {
      failures.add(failure);
    }
  }

  private static void deleteDirectory(String path) throws Exception {
    java.nio.file.Path dir = Paths.get(path);
    if (Files.exists(dir)) {
      try (Stream<java.nio.file.Path> walk = Files.walk(dir)) {
        walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
      }
    }
  }
}
