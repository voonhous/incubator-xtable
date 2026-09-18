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
 
package org.apache.xtable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

/**
 * Unshredded variant columns written by the Hudi Java client land in Iceberg as real variant
 * columns of a format-version 3 table and read back through Iceberg's own readers with their values
 * intact: at the top level (optional and required) and nested in a struct, a list and a map; on
 * copy-on-write and merge-on-read (deletion-vector model) tables, partitioned or not; across
 * inserts, updates, deletes, compaction, clustering, cleaning and rollback; and when a variant
 * column is added to an existing table. Tables that cannot hold a variant (format version 2) are
 * rejected before any Iceberg metadata is written.
 *
 * <p>The values are encoded with Iceberg's variant implementation and decoded with it again, so the
 * round trip exercises the Parquet layout Hudi writes ({@code metadata} and {@code value} binaries
 * under a group that carries the VARIANT annotation when parquet-mr supports it) rather than any
 * particular encoder. Reads go through Iceberg's Parquet readers with the table's name mapping and
 * Iceberg's delete filter, the way engines resolve Hudi files, which carry no field ids.
 */
class ITIcebergVariant {

  @TempDir static Path tempDir;

  private static final String PARTITION_CONFIG = "region:SIMPLE";

  private static Properties tableProperties(Integer formatVersion) {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.TABLE_FORMAT.key(), "ICEBERG");
    properties.put(
        HoodieTableConfig.VERSION.key(), String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
    properties.put(HoodieMetadataConfig.ENABLE.key(), "false");
    if (formatVersion != null) {
      properties.put("xtable.iceberg.format-version", String.valueOf(formatVersion));
    }
    // this test is about the unshredded layout; the Java writer never shreds, but be explicit
    properties.put("hoodie.parquet.variant.shredding.schema.inference.enabled", "false");
    properties.put("hoodie.parquet.variant.write.shredding.enabled", "false");
    return properties;
  }

  /** Merge-on-read under the deletion-vector model the Iceberg table format supports. */
  private static Properties tableProperties(HoodieTableType tableType) {
    Properties properties = tableProperties(3);
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      properties.put("hoodie.write.updates.as.deletes.and.inserts", "true");
      properties.put("hoodie.index.type", "SIMPLE");
    }
    return properties;
  }

  private static Schema plainSchema() {
    return SchemaBuilder.record("VariantTest")
        .namespace("test")
        .fields()
        .requiredString("key")
        .requiredLong("ts")
        .requiredString("name")
        .endRecord();
  }

  /** key, ts, name, v (optional variant): what a plain table can evolve into. */
  private static Schema evolvedSchema() {
    return SchemaBuilder.record("VariantTest")
        .namespace("test")
        .fields()
        .requiredString("key")
        .requiredLong("ts")
        .requiredString("name")
        .name("v")
        .type(nullable(variant("variant")))
        .withDefault(null)
        .endRecord();
  }

  /**
   * key, ts, name, region (partition column), long_field (the test harness's clustering sort
   * column), v (optional variant), v_req (required variant), s.inner (optional variant in a
   * struct), l (list of variants), m (map of optional variants).
   */
  private static Schema variantSchema() {
    Schema struct =
        SchemaBuilder.record("s_struct")
            .namespace("test.s")
            .fields()
            .name("inner")
            .type(nullable(variant("variant_inner")))
            .withDefault(null)
            .endRecord();
    return SchemaBuilder.record("VariantTest")
        .namespace("test")
        .fields()
        .requiredString("key")
        .requiredLong("ts")
        .requiredString("name")
        .requiredString("region")
        .requiredLong("long_field")
        .name("v")
        .type(nullable(variant("variant")))
        .withDefault(null)
        .name("v_req")
        .type(variant("variant_req"))
        .noDefault()
        .name("s")
        .type(struct)
        .noDefault()
        .name("l")
        .type(Schema.createArray(variant("variant_element")))
        .noDefault()
        .name("m")
        .type(Schema.createMap(nullable(variant("variant_map_value"))))
        .noDefault()
        .endRecord();
  }

  private static Schema variant(String name) {
    return HoodieSchema.createVariant(name, null, null).toAvroSchema();
  }

  private static Schema nullable(Schema schema) {
    return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
  }

  // ---------------------------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------------------------

  private static Stream<Arguments> tableLayouts() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, true),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true));
  }

  @ParameterizedTest(name = "{0}, partitioned={1}")
  @MethodSource("tableLayouts")
  void unshreddedVariantRoundTripsThroughIceberg(HoodieTableType tableType, boolean partitioned)
      throws Exception {
    Schema schema = variantSchema();
    boolean mergeOnRead = tableType == HoodieTableType.MERGE_ON_READ;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            "variant_" + tableType.name().toLowerCase() + (partitioned ? "_partitioned" : ""),
            tempDir,
            partitioned ? PARTITION_CONFIG : null,
            tableType,
            schema,
            tableProperties(tableType))) {
      String basePath = table.getBasePath();
      Map<String, Row> expected = new LinkedHashMap<>();

      List<HoodieRecord<HoodieAvroPayload>> inserts =
          toRecords(schema, apply(expected, firstBatch(), 1L), partitioned);
      table.insertRecordsAsIs(inserts, true);
      assertIcebergTable(basePath, expected, partitioned);

      table.upsertRecordsAsIs(
          toRecords(schema, apply(expected, secondBatch(), 2L), partitioned), true);
      Table icebergTable = assertIcebergTable(basePath, expected, partitioned);
      if (mergeOnRead) {
        assertTrue(
            summaryCount(icebergTable, SnapshotSummary.ADDED_DELETE_FILES_PROP) > 0,
            "merge-on-read updates must reach Iceberg as deletion vectors");
      }

      HoodieRecord<HoodieAvroPayload> deleted = inserts.get(2);
      table.deleteRecords(Collections.singletonList(deleted), true);
      expected.remove(deleted.getRecordKey());
      assertIcebergTable(basePath, expected, partitioned);

      if (mergeOnRead) {
        table.compact();
        icebergTable = assertIcebergTable(basePath, expected, partitioned);
        assertEquals(0, deleteFileCount(icebergTable), "compaction must fold the deletion vectors");
      }
    }
  }

  @Test
  void clusteringKeepsVariantValues() throws Exception {
    Schema schema = variantSchema();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            "variant_clustering",
            tempDir,
            PARTITION_CONFIG,
            HoodieTableType.MERGE_ON_READ,
            schema,
            tableProperties(HoodieTableType.MERGE_ON_READ))) {
      String basePath = table.getBasePath();
      Map<String, Row> expected = new LinkedHashMap<>();
      table.insertRecordsAsIs(toRecords(schema, apply(expected, firstBatch(), 1L), true), true);
      table.upsertRecordsAsIs(toRecords(schema, apply(expected, secondBatch(), 2L), true), true);

      table.cluster();
      Table icebergTable = assertIcebergTable(basePath, expected, true);
      assertEquals(0, deleteFileCount(icebergTable), "clustering rewrites away the deletes");

      // updates after clustering must keep working against the clustered base files
      table.upsertRecordsAsIs(toRecords(schema, apply(expected, thirdBatch(), 3L), true), true);
      assertIcebergTable(basePath, expected, true);
    }
  }

  @Test
  void cleaningKeepsVariantValues() throws Exception {
    Schema schema = variantSchema();
    Properties properties = tableProperties(HoodieTableType.MERGE_ON_READ);
    properties.put("hoodie.cleaner.commits.retained", "1");
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            "variant_cleaning",
            tempDir,
            PARTITION_CONFIG,
            HoodieTableType.MERGE_ON_READ,
            schema,
            properties)) {
      String basePath = table.getBasePath();
      Map<String, Row> expected = new LinkedHashMap<>();
      table.insertRecordsAsIs(toRecords(schema, apply(expected, firstBatch(), 1L), true), true);
      // two update-compact cycles leave replaced file slices for the cleaner
      table.upsertRecordsAsIs(toRecords(schema, apply(expected, secondBatch(), 2L), true), true);
      table.compact();
      table.upsertRecordsAsIs(toRecords(schema, apply(expected, thirdBatch(), 3L), true), true);
      table.compact();
      table.getWriteClient().clean();
      assertIcebergTable(basePath, expected, true);
    }
  }

  @Test
  void rollbackRestoresVariantValues() throws Exception {
    Schema schema = variantSchema();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            "variant_rollback",
            tempDir,
            PARTITION_CONFIG,
            HoodieTableType.MERGE_ON_READ,
            schema,
            tableProperties(HoodieTableType.MERGE_ON_READ))) {
      String basePath = table.getBasePath();
      Map<String, Row> expected = new LinkedHashMap<>();
      table.insertRecordsAsIs(toRecords(schema, apply(expected, firstBatch(), 1L), true), true);
      Map<String, Row> beforeUpdate = new LinkedHashMap<>(expected);
      table.upsertRecordsAsIs(toRecords(schema, apply(expected, secondBatch(), 2L), true), true);
      assertIcebergTable(basePath, expected, true);

      String updateInstant =
          table
              .getMetaClient()
              .reloadActiveTimeline()
              .filterCompletedInstants()
              .lastInstant()
              .get()
              .requestedTime();
      assertTrue(table.getWriteClient().rollback(updateInstant), "rollback must succeed");
      Table icebergTable = assertIcebergTable(basePath, beforeUpdate, true);
      assertEquals(0, deleteFileCount(icebergTable), "rollback must remove the deletion vectors");
    }
  }

  @Test
  void addingAVariantColumnToAFormatVersionThreeTable() throws Exception {
    String tableName = "variant_evolution_v3";
    List<HoodieRecord<HoodieAvroPayload>> plainRows = plainRecords(plainSchema(), 3);
    String basePath;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            tableName,
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            plainSchema(),
            tableProperties(3))) {
      table.insertRecordsAsIs(plainRows, true);
      basePath = table.getBasePath();
    }
    Table before = new HadoopTables(new Configuration()).load(basePath);
    assertEquals(3, ((BaseTable) before).operations().current().formatVersion());
    assertNull(before.schema().findField("v"));

    // the same table name maps to the same base path, so this reopens the table with the
    // evolved schema (only a nullable column can be added to an existing Hudi table)
    Schema evolved = evolvedSchema();
    Map<String, Row> added = new LinkedHashMap<>();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE, evolved, tableProperties(3))) {
      table.insertRecordsAsIs(toRecords(evolved, apply(added, secondBatch(), 2L), false), true);
    }

    Table after = new HadoopTables(new Configuration()).load(basePath);
    assertEquals(Types.VariantType.get(), after.schema().findType("v"));
    assertTrue(after.schema().findField("v").isOptional());
    Map<String, Record> rows = readRows(after);
    assertEquals(plainRows.size() + added.size(), rows.size());
    plainRows.forEach(
        row ->
            assertNull(
                rows.get(row.getRecordKey()).getField("v"),
                "rows written before the column existed read as SQL null"));
    added.forEach((key, row) -> row.v.checkRead(key, rows.get(key).getField("v")));
  }

  @Test
  void formatVersionTwoIsRejectedBeforeAnyIcebergMetadataIsWritten() throws Exception {
    Schema schema = variantSchema();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            "variant_v2",
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            schema,
            tableProperties(2))) {
      List<HoodieRecord<HoodieAvroPayload>> inserts =
          toRecords(schema, apply(new LinkedHashMap<>(), firstBatch(), 1L), false);
      Exception ex = assertThrows(Exception.class, () -> table.insertRecordsAsIs(inserts, true));
      assertTrue(rootMessages(ex).contains("require Iceberg format version 3"), rootMessages(ex));
      assertFalse(
          new HadoopTables(new Configuration()).exists(table.getBasePath()),
          "no Iceberg table must be created for a schema the format version cannot hold");
    }
  }

  @Test
  void addingAVariantColumnToAFormatVersionTwoTableFails() throws Exception {
    String tableName = "variant_evolution_v2";
    List<HoodieRecord<HoodieAvroPayload>> plainRows = plainRecords(plainSchema(), 3);
    String basePath;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            tableName,
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            plainSchema(),
            tableProperties((Integer) null))) {
      table.insertRecordsAsIs(plainRows, true);
      basePath = table.getBasePath();
    }
    Table before = new HadoopTables(new Configuration()).load(basePath);
    assertEquals(2, ((BaseTable) before).operations().current().formatVersion());

    Schema evolved = evolvedSchema();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            tableName,
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            evolved,
            tableProperties((Integer) null))) {
      assertEquals(basePath, table.getBasePath());
      List<HoodieRecord<HoodieAvroPayload>> inserts =
          toRecords(evolved, apply(new LinkedHashMap<>(), secondBatch(), 2L), false);
      Exception ex = assertThrows(Exception.class, () -> table.insertRecordsAsIs(inserts, true));
      assertTrue(rootMessages(ex).contains("is at format version 2"), rootMessages(ex));
    }
    Table after = new HadoopTables(new Configuration()).load(basePath);
    assertNull(after.schema().findField("v"), "the variant column must not reach the v2 table");
    assertEquals(before.currentSnapshot().snapshotId(), after.currentSnapshot().snapshotId());
  }

  // ---------------------------------------------------------------------------------------------
  // Assertions on the Iceberg side
  // ---------------------------------------------------------------------------------------------

  /** Checks the Iceberg schema, name mapping, file layout and every value; returns the table. */
  private static Table assertIcebergTable(
      String basePath, Map<String, Row> expected, boolean partitioned) throws IOException {
    Table table = new HadoopTables(new Configuration()).load(basePath);
    assertEquals(3, ((BaseTable) table).operations().current().formatVersion());
    assertEquals(partitioned, table.spec().isPartitioned(), "partition spec " + table.spec());
    for (String column : Arrays.asList("v", "v_req", "s.inner", "l.element", "m.value")) {
      assertEquals(Types.VariantType.get(), table.schema().findType(column), column);
    }
    assertTrue(table.schema().findField("v").isOptional());
    assertTrue(table.schema().findField("v_req").isRequired());
    // nullability of nested variants must match the Hudi schema, which readers rely on
    assertTrue(table.schema().findField("s.inner").isOptional(), "s.inner holds SQL nulls");
    assertTrue(table.schema().findField("l.element").isRequired(), "l has no null elements");
    assertTrue(table.schema().findField("m.value").isOptional(), "m holds SQL null values");

    NameMapping mapping =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    for (String[] column :
        Arrays.asList(
            new String[] {"v"},
            new String[] {"v_req"},
            new String[] {"s", "inner"},
            new String[] {"l", "element"},
            new String[] {"m", "value"})) {
      MappedField mapped = mapping.find(column);
      assertNotNull(mapped, "name mapping for " + String.join(".", column));
      assertEquals(table.schema().findField(String.join(".", column)).fieldId(), mapped.id());
      assertNull(
          mapped.nestedMapping(),
          "the metadata and value components of a variant carry no field ids");
    }

    assertParquetLayout(table);
    Map<String, Record> rows = readRows(table);
    assertEquals(expected.keySet(), rows.keySet());
    expected.forEach((key, row) -> row.check(key, rows.get(key)));
    return table;
  }

  /**
   * Hudi stamps the Parquet VARIANT annotation on a variant group whenever the parquet-mr on its
   * classpath can express it (1.16 and later), and writes a plain metadata/value group otherwise.
   * CI runs this test with both, so check that each run really produced the layout it is meant to
   * cover, at every depth: Iceberg must read both.
   */
  private static void assertParquetLayout(Table table) throws IOException {
    boolean annotationAvailable =
        Arrays.stream(LogicalTypeAnnotation.class.getMethods())
            .anyMatch(method -> method.getName().equals("variantType"));
    Configuration conf = new Configuration();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        try (ParquetFileReader reader =
            ParquetFileReader.open(
                HadoopInputFile.fromPath(
                    new org.apache.hadoop.fs.Path(task.file().location()), conf))) {
          MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
          for (String[] path :
              Arrays.asList(
                  new String[] {"v"},
                  new String[] {"v_req"},
                  new String[] {"s", "inner"},
                  new String[] {"l", "list", "element"},
                  new String[] {"m", "key_value", "value"})) {
            Type group = fileSchema;
            for (String name : path) {
              group = ((GroupType) group).getType(name);
            }
            LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
            assertEquals(
                annotationAvailable,
                annotation != null && annotation.toString().startsWith("VARIANT"),
                String.format(
                    "column %s of %s: annotation %s, but parquet-mr %s the VARIANT annotation",
                    String.join(".", path),
                    task.file().location(),
                    annotation,
                    annotationAvailable ? "supports" : "does not support"));
          }
        }
      }
    }
  }

  /**
   * Reads the live rows the way an engine does: every data file through Iceberg's Parquet readers
   * with the table's name mapping (Hudi files carry no field ids, and {@code IcebergGenerics} does
   * not apply the mapping), with the table's deletion vectors applied by Iceberg's delete filter.
   * Fails on a key seen twice, which is what a replaced file left in the snapshot looks like.
   */
  private static Map<String, Record> readRows(Table table) throws IOException {
    NameMapping mapping =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    Map<String, Record> rows = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        GenericDeleteFilter deletes =
            new GenericDeleteFilter(table.io(), task, table.schema(), table.schema());
        org.apache.iceberg.Schema readSchema = deletes.requiredSchema();
        try (CloseableIterable<Record> records =
            deletes.filter(
                Parquet.read(table.io().newInputFile(task.file().location()))
                    .project(readSchema)
                    .withNameMapping(mapping)
                    .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema))
                    .build())) {
          for (Record record : records) {
            String key = record.getField("key").toString();
            assertNull(rows.put(key, record.copy()), "key " + key + " read twice from Iceberg");
          }
        }
      }
    }
    return rows;
  }

  private static long summaryCount(Table table, String property) {
    return Long.parseLong(table.currentSnapshot().summary().getOrDefault(property, "0"));
  }

  private static long deleteFileCount(Table table) throws IOException {
    long count = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        count += task.deletes().size();
      }
    }
    return count;
  }

  private static String rootMessages(Throwable t) {
    StringBuilder messages = new StringBuilder();
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      messages.append(cause.getMessage()).append(" | ");
    }
    return messages.toString();
  }

  // ---------------------------------------------------------------------------------------------
  // Test data: variant values built with Iceberg's encoder, written as Hudi records
  // ---------------------------------------------------------------------------------------------

  /** A variant value (null means SQL null in the optional column) and its semantic check. */
  private static final class VariantSpec {
    final VariantMetadata metadata;
    final VariantValue value;
    final Consumer<VariantValue> check;

    VariantSpec(VariantMetadata metadata, VariantValue value, Consumer<VariantValue> check) {
      this.metadata = metadata;
      this.value = value;
      this.check = check;
    }

    static VariantSpec sqlNull() {
      return new VariantSpec(null, null, v -> {});
    }

    static VariantSpec of(VariantValue value) {
      return new VariantSpec(Variants.emptyMetadata(), value, v -> {});
    }

    void checkRead(String context, Object read) {
      if (value == null) {
        assertNull(read, "SQL null expected for " + context);
        return;
      }
      assertTrue(read instanceof Variant, "variant expected for " + context + " but got " + read);
      check.accept(((Variant) read).value());
    }
  }

  /**
   * One live row: its top-level optional variant and the commit (ts) that wrote it. The required
   * and nested variants are derived from the key and ts, so every update changes them too.
   */
  private static final class Row {
    final VariantSpec v;
    final long ts;

    Row(VariantSpec v, long ts) {
      this.v = v;
      this.ts = ts;
    }

    String tag(String key) {
      return key + "@" + ts;
    }

    /** The struct's variant: SQL null for k6, a string otherwise. */
    VariantSpec inner(String key) {
      return key.equals("k6")
          ? VariantSpec.sqlNull()
          : VariantSpec.of(Variants.of("inner-" + tag(key)));
    }

    /** List elements: empty for k5, otherwise the row number and the tag. */
    List<VariantValue> elements(String key) {
      return key.equals("k5")
          ? Collections.emptyList()
          : Arrays.asList(Variants.of(rowNumber(key)), Variants.of(tag(key)));
    }

    /** Map values: n holds the row number, "none" is a SQL null value. */
    Map<String, VariantSpec> mapValues(String key) {
      Map<String, VariantSpec> values = new LinkedHashMap<>();
      values.put("n", VariantSpec.of(Variants.of(rowNumber(key))));
      values.put("none", VariantSpec.sqlNull());
      return values;
    }

    void check(String key, Record record) {
      v.checkRead(key + ".v", record.getField("v"));
      Object required = record.getField("v_req");
      assertTrue(required instanceof Variant, "v_req of " + key);
      assertEquals(rowNumber(key), ((Variant) required).value().asPrimitive().get(), key);

      inner(key).checkRead(key + ".s.inner", ((Record) record.getField("s")).getField("inner"));
      if (inner(key).value != null) {
        assertEquals(
            "inner-" + tag(key),
            ((Variant) ((Record) record.getField("s")).getField("inner"))
                .value()
                .asPrimitive()
                .get());
      }

      List<?> list = (List<?>) record.getField("l");
      List<Object> expectedElements =
          elements(key).stream().map(e -> e.asPrimitive().get()).collect(Collectors.toList());
      List<Object> readElements =
          list.stream()
              .map(e -> ((Variant) e).value().asPrimitive().get())
              .collect(Collectors.toList());
      assertEquals(expectedElements, readElements, key + ".l");

      Map<?, ?> map = (Map<?, ?>) record.getField("m");
      Map<String, VariantSpec> expectedValues = mapValues(key);
      assertEquals(expectedValues.keySet(), keysAsStrings(map), key + ".m keys");
      for (Map.Entry<?, ?> e : map.entrySet()) {
        String mapKey = e.getKey().toString();
        expectedValues.get(mapKey).checkRead(key + ".m." + mapKey, e.getValue());
        if (e.getValue() != null) {
          assertEquals(rowNumber(key), ((Variant) e.getValue()).value().asPrimitive().get());
        }
      }
    }
  }

  private static java.util.Set<String> keysAsStrings(Map<?, ?> map) {
    return map.keySet().stream().map(Object::toString).collect(Collectors.toSet());
  }

  /** Records the batch as the new live state of its keys and returns the rows to write. */
  private static Map<String, Row> apply(
      Map<String, Row> live, Map<String, VariantSpec> batch, long ts) {
    Map<String, Row> written = new LinkedHashMap<>();
    batch.forEach((key, spec) -> written.put(key, new Row(spec, ts)));
    live.putAll(written);
    return written;
  }

  private static Map<String, VariantSpec> firstBatch() {
    Map<String, VariantSpec> specs = new LinkedHashMap<>();

    VariantMetadata ab = Variants.metadata("a", "b");
    ShreddedObject object = Variants.object(ab);
    object.put("a", Variants.of(1));
    object.put("b", Variants.of("x"));
    specs.put(
        "k1",
        new VariantSpec(
            ab,
            object,
            v -> {
              assertEquals(PhysicalType.OBJECT, v.type());
              assertEquals(1, v.asObject().get("a").asPrimitive().get());
              assertEquals("x", v.asObject().get("b").asPrimitive().get());
            }));

    ValueArray array = Variants.array();
    array.add(Variants.of(1));
    array.add(Variants.of(2));
    array.add(Variants.of(3));
    specs.put(
        "k2",
        new VariantSpec(
            Variants.emptyMetadata(),
            array,
            v -> {
              assertEquals(PhysicalType.ARRAY, v.type());
              assertEquals(3, v.asArray().numElements());
              assertEquals(3, v.asArray().get(2).asPrimitive().get());
            }));

    VariantMetadata nestedMetadata = Variants.metadata("a", "nested", "d");
    ShreddedObject inner = Variants.object(nestedMetadata);
    inner.put("nested", Variants.of(true));
    ShreddedObject outer = Variants.object(nestedMetadata);
    outer.put("a", inner);
    outer.put("d", Variants.of(1.5d));
    specs.put(
        "k3",
        new VariantSpec(
            nestedMetadata,
            outer,
            v -> {
              assertEquals(
                  true, v.asObject().get("a").asObject().get("nested").asPrimitive().get());
              assertEquals(1.5d, v.asObject().get("d").asPrimitive().get());
            }));

    specs.put(
        "k4",
        new VariantSpec(
            Variants.emptyMetadata(),
            Variants.of("plain string"),
            v -> assertEquals("plain string", v.asPrimitive().get())));
    specs.put(
        "k5",
        new VariantSpec(
            Variants.emptyMetadata(),
            Variants.ofNull(),
            v -> assertEquals(PhysicalType.NULL, v.type())));
    specs.put("k6", VariantSpec.sqlNull());
    specs.put(
        "k7",
        new VariantSpec(
            Variants.emptyMetadata(),
            Variants.of(new BigDecimal("12.34")),
            v ->
                assertEquals(
                    0, new BigDecimal("12.34").compareTo((BigDecimal) v.asPrimitive().get()))));
    specs.put(
        "k8",
        new VariantSpec(
            Variants.emptyMetadata(),
            Variants.of(9007199254740993L),
            v -> assertEquals(9007199254740993L, v.asPrimitive().get())));
    return specs;
  }

  private static Map<String, VariantSpec> secondBatch() {
    Map<String, VariantSpec> specs = new LinkedHashMap<>();
    VariantMetadata abc = Variants.metadata("a", "b", "c");
    ShreddedObject object = Variants.object(abc);
    object.put("a", Variants.of(11));
    object.put("b", Variants.of("y"));
    object.put("c", Variants.of(false));
    specs.put(
        "k1",
        new VariantSpec(
            abc,
            object,
            v -> {
              assertEquals(11, v.asObject().get("a").asPrimitive().get());
              assertEquals(false, v.asObject().get("c").asPrimitive().get());
            }));
    specs.put(
        "k2",
        new VariantSpec(
            Variants.emptyMetadata(),
            Variants.of(2.5d),
            v -> assertEquals(2.5d, v.asPrimitive().get())));
    specs.put(
        "k9",
        new VariantSpec(
            Variants.emptyMetadata(),
            Variants.ofIsoDate("2024-01-02"),
            v -> assertEquals(PhysicalType.DATE, v.type())));
    return specs;
  }

  private static Map<String, VariantSpec> thirdBatch() {
    Map<String, VariantSpec> specs = new LinkedHashMap<>();
    specs.put(
        "k1",
        new VariantSpec(
            Variants.emptyMetadata(),
            Variants.of("third"),
            v -> assertEquals("third", v.asPrimitive().get())));
    specs.put("k4", VariantSpec.sqlNull());
    return specs;
  }

  private static int rowNumber(String key) {
    return Integer.parseInt(key.substring(1));
  }

  private static String region(String key) {
    return rowNumber(key) % 2 == 0 ? "r0" : "r1";
  }

  private static List<HoodieRecord<HoodieAvroPayload>> toRecords(
      Schema schema, Map<String, Row> rows, boolean partitioned) {
    List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
    for (Map.Entry<String, Row> e : rows.entrySet()) {
      String key = e.getKey();
      Row row = e.getValue();
      GenericRecord record = new GenericData.Record(schema);
      record.put("key", key);
      record.put("ts", row.ts);
      record.put("name", key);
      record.put("v", row.v.value == null ? null : variantRecord(fieldSchema(schema, "v"), row.v));
      if (schema.getField("region") != null) {
        record.put("region", region(key));
        record.put("long_field", (long) rowNumber(key));
        record.put(
            "v_req",
            variantRecord(
                fieldSchema(schema, "v_req"), VariantSpec.of(Variants.of(rowNumber(key)))));

        Schema structSchema = fieldSchema(schema, "s");
        GenericRecord struct = new GenericData.Record(structSchema);
        VariantSpec inner = row.inner(key);
        struct.put(
            "inner",
            inner.value == null ? null : variantRecord(fieldSchema(structSchema, "inner"), inner));
        record.put("s", struct);

        Schema elementSchema = fieldSchema(schema, "l").getElementType();
        record.put(
            "l",
            row.elements(key).stream()
                .map(value -> variantRecord(elementSchema, VariantSpec.of(value)))
                .collect(Collectors.toList()));

        Schema mapValueSchema = nonNull(fieldSchema(schema, "m").getValueType());
        Map<String, Object> map = new LinkedHashMap<>();
        row.mapValues(key)
            .forEach(
                (mapKey, spec) ->
                    map.put(
                        mapKey, spec.value == null ? null : variantRecord(mapValueSchema, spec)));
        record.put("m", map);
      }
      records.add(
          new HoodieAvroRecord<>(
              new HoodieKey(key, partitioned ? region(key) : ""),
              new HoodieAvroPayload(Option.of(record))));
    }
    return records;
  }

  private static List<HoodieRecord<HoodieAvroPayload>> plainRecords(Schema schema, int count) {
    List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("key", "p" + i);
      record.put("ts", 1L);
      record.put("name", "plain " + i);
      records.add(
          new HoodieAvroRecord<>(
              new HoodieKey("p" + i, ""), new HoodieAvroPayload(Option.of(record))));
    }
    return records;
  }

  /** The field's schema with a nullable union unwrapped. */
  private static Schema fieldSchema(Schema record, String field) {
    return nonNull(record.getField(field).schema());
  }

  private static Schema nonNull(Schema schema) {
    return schema.getType() == Schema.Type.UNION
        ? schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).findFirst().get()
        : schema;
  }

  private static GenericRecord variantRecord(Schema variantSchema, VariantSpec spec) {
    GenericRecord record = new GenericData.Record(variantSchema);
    record.put("metadata", serialize(spec.metadata.sizeInBytes(), spec.metadata::writeTo));
    record.put("value", serialize(spec.value.sizeInBytes(), spec.value::writeTo));
    return record;
  }

  private interface Serializer {
    int writeTo(ByteBuffer buffer, int offset);
  }

  private static ByteBuffer serialize(int size, Serializer serializer) {
    ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    serializer.writeTo(buffer, 0);
    buffer.position(0);
    buffer.limit(size);
    return buffer;
  }
}
