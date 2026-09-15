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
 
package org.apache.xtable.iceberg;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;

/** Variant columns on the Iceberg side: type mapping, name mapping, and the v3 safeguards. */
public class TestIcebergVariantSupport {
  private static final IcebergSchemaExtractor EXTRACTOR = IcebergSchemaExtractor.getInstance();

  private static final Schema PLAIN_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final Schema NESTED_VARIANT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "v", Types.VariantType.get()),
          Types.NestedField.required(
              3,
              "s",
              Types.StructType.of(Types.NestedField.optional(4, "inner", Types.VariantType.get()))),
          Types.NestedField.optional(5, "l", Types.ListType.ofRequired(6, Types.VariantType.get())),
          Types.NestedField.optional(
              7,
              "m",
              Types.MapType.ofOptional(8, 9, Types.StringType.get(), Types.VariantType.get())));

  private static InternalSchema variant(boolean nullable) {
    return InternalSchema.builder()
        .name("variant")
        .dataType(InternalType.VARIANT)
        .isNullable(nullable)
        .build();
  }

  private static InternalField field(String name, InternalSchema schema) {
    return InternalField.builder().name(name).schema(schema).build();
  }

  private static InternalSchema internalSchemaWithVariants() {
    InternalSchema string =
        InternalSchema.builder().name("string").dataType(InternalType.STRING).build();
    return InternalSchema.builder()
        .name("record")
        .dataType(InternalType.RECORD)
        .fields(
            Arrays.asList(
                field("v", variant(true)),
                field("v_req", variant(false)),
                field(
                    "s",
                    InternalSchema.builder()
                        .name("s_type")
                        .dataType(InternalType.RECORD)
                        .fields(
                            Collections.singletonList(
                                InternalField.builder()
                                    .name("inner")
                                    .parentPath("s")
                                    .schema(variant(false))
                                    .build()))
                        .build()),
                field(
                    "l",
                    InternalSchema.builder()
                        .name("array")
                        .dataType(InternalType.LIST)
                        .fields(
                            Collections.singletonList(
                                InternalField.builder()
                                    .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                    .parentPath("l")
                                    .schema(variant(false))
                                    .build()))
                        .build()),
                field(
                    "m",
                    InternalSchema.builder()
                        .name("map")
                        .dataType(InternalType.MAP)
                        .fields(
                            Arrays.asList(
                                InternalField.builder()
                                    .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                                    .parentPath("m")
                                    .schema(string)
                                    .build(),
                                InternalField.builder()
                                    .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                    .parentPath("m")
                                    .schema(variant(true))
                                    .build()))
                        .build())))
        .build();
  }

  @Test
  void variantMapsToIcebergVariantTypeAtAnyDepth() {
    Schema iceberg = EXTRACTOR.toIceberg(internalSchemaWithVariants());

    assertEquals(Types.VariantType.get(), iceberg.findType("v"));
    assertTrue(iceberg.findField("v").isOptional());
    assertEquals(Types.VariantType.get(), iceberg.findType("v_req"));
    assertTrue(iceberg.findField("v_req").isRequired());
    assertEquals(Types.VariantType.get(), iceberg.findType("s.inner"));
    assertEquals(Types.VariantType.get(), iceberg.findType("l.element"));
    assertEquals(Types.VariantType.get(), iceberg.findType("m.value"));

    // the name mapping gives the variant an id and nothing to its metadata/value components
    NameMapping mapping = MappingUtil.create(iceberg);
    MappedField variantMapping = mapping.find("v");
    assertNotNull(variantMapping);
    assertEquals(iceberg.findField("v").fieldId(), variantMapping.id());
    assertNull(variantMapping.nestedMapping());

    InternalSchema roundTrip = EXTRACTOR.fromIceberg(iceberg);
    assertEquals(InternalType.VARIANT, internalField(roundTrip, "v").getSchema().getDataType());
    assertTrue(internalField(roundTrip, "v").getSchema().isNullable());
    assertEquals(InternalType.VARIANT, internalField(roundTrip, "v_req").getSchema().getDataType());
    assertFalse(internalField(roundTrip, "v_req").getSchema().isNullable());
  }

  @Test
  void variantColumnsAreFoundAtAnyDepth() {
    assertEquals(
        Arrays.asList("v", "s.inner", "l.element", "m.value"),
        IcebergVariantSupport.variantColumns(NESTED_VARIANT_SCHEMA));
    assertTrue(IcebergVariantSupport.variantColumns(PLAIN_SCHEMA).isEmpty());
  }

  @Test
  void variantRequiresFormatVersionThree() {
    NotSupportedException ex =
        assertThrows(
            NotSupportedException.class,
            () ->
                IcebergVariantSupport.requireFormatVersion(
                    NESTED_VARIANT_SCHEMA, 2, "the table is at version 2"));
    assertTrue(ex.getMessage().contains("[v, s.inner, l.element, m.value]"), ex.getMessage());
    assertTrue(ex.getMessage().contains("format version 3"), ex.getMessage());
    assertTrue(ex.getMessage().contains("the table is at version 2"), ex.getMessage());

    assertDoesNotThrow(
        () -> IcebergVariantSupport.requireFormatVersion(NESTED_VARIANT_SCHEMA, 3, "v3"));
    assertDoesNotThrow(() -> IcebergVariantSupport.requireFormatVersion(PLAIN_SCHEMA, 1, "v1"));
  }

  @Test
  void partitioningByVariantIsRejected() {
    Schema tableSchema = new Schema(Types.NestedField.optional(1, "v", Types.VariantType.get()));
    for (PartitionTransformType transform :
        Arrays.asList(PartitionTransformType.VALUE, PartitionTransformType.BUCKET)) {
      InternalPartitionField partitionField =
          InternalPartitionField.builder()
              .sourceField(field("v", variant(true)))
              .transformType(transform)
              .transformOptions(Collections.singletonMap(InternalPartitionField.NUM_BUCKETS, 4))
              .build();
      NotSupportedException ex =
          assertThrows(
              NotSupportedException.class,
              () ->
                  IcebergPartitionSpecExtractor.getInstance()
                      .toIceberg(Collections.singletonList(partitionField), tableSchema));
      assertTrue(ex.getMessage().contains("variant column v"), ex.getMessage());
    }
  }

  @Test
  void variantStatsKeepCountsButNoBounds() {
    Schema tableSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "v", Types.VariantType.get()));
    ColumnStat idStats =
        ColumnStat.builder()
            .field(
                field(
                    "id", InternalSchema.builder().name("int").dataType(InternalType.INT).build()))
            .numValues(10)
            .numNulls(0)
            .totalSize(40)
            .range(Range.vector(1, 10))
            .build();
    ColumnStat variantStats =
        ColumnStat.builder()
            .field(field("v", variant(true)))
            .numValues(10)
            .numNulls(3)
            .totalSize(400)
            .range(
                Range.vector(
                    ByteBuffer.wrap(new byte[] {1, 0, 0}), ByteBuffer.wrap(new byte[] {1, 2, 3})))
            .build();

    Metrics metrics =
        IcebergColumnStatsConverter.getInstance()
            .toIceberg(tableSchema, 10, Arrays.asList(idStats, variantStats));

    assertEquals(10L, metrics.valueCounts().get(2));
    assertEquals(3L, metrics.nullValueCounts().get(2));
    assertEquals(400L, metrics.columnSizes().get(2));
    assertFalse(metrics.lowerBounds().containsKey(2), "no lower bound for a variant column");
    assertFalse(metrics.upperBounds().containsKey(2), "no upper bound for a variant column");
    assertTrue(metrics.lowerBounds().containsKey(1), "other columns keep their bounds");
  }

  private static InternalField internalField(InternalSchema schema, String name) {
    return schema.getFields().stream()
        .filter(f -> f.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no field " + name));
  }
}
