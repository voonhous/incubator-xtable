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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
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
 * intact, across inserts, updates and deletes. Tables that cannot hold a variant (format version 2)
 * are rejected before any Iceberg metadata is written.
 *
 * <p>The values are encoded with Iceberg's variant implementation and decoded with it again, so the
 * round trip exercises the Parquet layout Hudi writes ({@code metadata} and {@code value} binaries
 * under a group that carries the VARIANT annotation when parquet-mr supports it) rather than any
 * particular encoder.
 */
class ITIcebergVariant {

  @TempDir static Path tempDir;

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
    Schema variant = HoodieSchema.createVariant().toAvroSchema();
    return SchemaBuilder.record("VariantTest")
        .namespace("test")
        .fields()
        .requiredString("key")
        .requiredLong("ts")
        .requiredString("name")
        .name("v")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), variant))
        .withDefault(null)
        .endRecord();
  }

  /** key, ts, name, v (optional variant), v_req (required variant). */
  private static Schema variantSchema() {
    Schema variant = HoodieSchema.createVariant().toAvroSchema();
    Schema nullableVariant = Schema.createUnion(Schema.create(Schema.Type.NULL), variant);
    return SchemaBuilder.record("VariantTest")
        .namespace("test")
        .fields()
        .requiredString("key")
        .requiredLong("ts")
        .requiredString("name")
        .name("v")
        .type(nullableVariant)
        .withDefault(null)
        .name("v_req")
        .type(HoodieSchema.createVariant("variant_req", null, null).toAvroSchema())
        .noDefault()
        .endRecord();
  }

  @Test
  void unshreddedVariantRoundTripsThroughIceberg() throws Exception {
    Schema schema = variantSchema();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            "variant_round_trip",
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            schema,
            tableProperties(3))) {
      String basePath = table.getBasePath();

      Map<String, VariantSpec> expected = new LinkedHashMap<>(firstBatch());
      List<HoodieRecord<HoodieAvroPayload>> inserts = toRecords(schema, expected);
      table.insertRecordsAsIs(inserts, true);
      assertIcebergTable(basePath, expected);

      Map<String, VariantSpec> updates = secondBatch();
      table.upsertRecordsAsIs(toRecords(schema, updates), true);
      expected.putAll(updates);
      assertIcebergTable(basePath, expected);

      HoodieRecord<HoodieAvroPayload> deleted = inserts.get(2);
      table.deleteRecords(Collections.singletonList(deleted), true);
      expected.remove(deleted.getRecordKey());
      assertIcebergTable(basePath, expected);
    }
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
      List<HoodieRecord<HoodieAvroPayload>> inserts = toRecords(schema, firstBatch());
      Exception ex = assertThrows(Exception.class, () -> table.insertRecordsAsIs(inserts, true));
      assertTrue(rootMessages(ex).contains("require Iceberg format version 3"), rootMessages(ex));
      assertFalse(
          new HadoopTables(new Configuration()).exists(table.getBasePath()),
          "no Iceberg table must be created for a schema the format version cannot hold");
    }
  }

  @Test
  void addingAVariantColumnToAFormatVersionTwoTableFails() throws Exception {
    String tableName = "variant_evolution";
    List<HoodieRecord<HoodieAvroPayload>> plainRows = plainRecords(plainSchema(), 3);
    String basePath;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            tableName,
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            plainSchema(),
            tableProperties(null))) {
      table.insertRecordsAsIs(plainRows, true);
      basePath = table.getBasePath();
    }
    Table before = new HadoopTables(new Configuration()).load(basePath);
    assertEquals(2, ((BaseTable) before).operations().current().formatVersion());

    // the same table name maps to the same base path, so this reopens the table with the
    // evolved schema (only a nullable column can be added to an existing Hudi table)
    Schema evolved = evolvedSchema();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withSchema(
            tableName,
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            evolved,
            tableProperties(null))) {
      assertEquals(basePath, table.getBasePath());
      List<HoodieRecord<HoodieAvroPayload>> inserts = toRecords(evolved, secondBatch());
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

  private static void assertIcebergTable(String basePath, Map<String, VariantSpec> expected)
      throws IOException {
    Table table = new HadoopTables(new Configuration()).load(basePath);
    assertEquals(3, ((BaseTable) table).operations().current().formatVersion());
    assertEquals(Types.VariantType.get(), table.schema().findType("v"));
    assertTrue(table.schema().findField("v").isOptional());
    assertEquals(Types.VariantType.get(), table.schema().findType("v_req"));
    assertTrue(table.schema().findField("v_req").isRequired());

    String mappingJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping mapping = NameMappingParser.fromJson(mappingJson);
    MappedField variantMapping = mapping.find("v");
    assertEquals(table.schema().findField("v").fieldId(), variantMapping.id());
    assertNull(
        variantMapping.nestedMapping(),
        "the metadata and value components of a variant carry no field ids");

    Map<String, Record> rows = readWithNameMapping(table, mapping);
    assertEquals(expected.keySet(), rows.keySet());
    for (Map.Entry<String, VariantSpec> e : expected.entrySet()) {
      Record row = rows.get(e.getKey());
      Object value = row.getField("v");
      if (e.getValue().value == null) {
        assertNull(value, "SQL null for " + e.getKey());
      } else {
        assertTrue(
            value instanceof Variant, "variant value for " + e.getKey() + " but got " + value);
        e.getValue().check.accept(((Variant) value).value());
      }
      Object required = row.getField("v_req");
      assertTrue(required instanceof Variant, "v_req of " + e.getKey());
      assertEquals(rowNumber(e.getKey()), ((Variant) required).value().asPrimitive().get());
    }
  }

  /**
   * Reads every data file through Iceberg's Parquet readers with the table's name mapping, the way
   * engines resolve files that carry no field ids ({@code IcebergGenerics} does not apply it).
   */
  private static Map<String, Record> readWithNameMapping(Table table, NameMapping mapping)
      throws IOException {
    Map<String, Record> rows = new LinkedHashMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        assertTrue(task.deletes().isEmpty(), "copy-on-write tables carry no delete files");
        try (CloseableIterable<Record> records =
            Parquet.read(table.io().newInputFile(task.file().location()))
                .project(table.schema())
                .withNameMapping(mapping)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build()) {
          for (Record record : records) {
            rows.put(record.getField("key").toString(), record);
          }
        }
      }
    }
    return rows;
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

  private static int rowNumber(String key) {
    return Integer.parseInt(key.substring(1));
  }

  private static List<HoodieRecord<HoodieAvroPayload>> toRecords(
      Schema schema, Map<String, VariantSpec> specs) {
    Schema optionalVariant =
        schema.getField("v").schema().getTypes().stream()
            .filter(s -> s.getType() != Schema.Type.NULL)
            .findFirst()
            .get();
    Schema.Field requiredField = schema.getField("v_req");
    List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
    for (Map.Entry<String, VariantSpec> e : specs.entrySet()) {
      VariantSpec spec = e.getValue();
      GenericRecord record = new GenericData.Record(schema);
      record.put("key", e.getKey());
      record.put("ts", 1L);
      record.put("name", e.getKey());
      record.put("v", spec.value == null ? null : variantRecord(optionalVariant, spec));
      if (requiredField != null) {
        VariantSpec rowNumber =
            new VariantSpec(Variants.emptyMetadata(), Variants.of(rowNumber(e.getKey())), v -> {});
        record.put("v_req", variantRecord(requiredField.schema(), rowNumber));
      }
      records.add(
          new HoodieAvroRecord<>(
              new HoodieKey(e.getKey(), ""), new HoodieAvroPayload(Option.of(record))));
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
