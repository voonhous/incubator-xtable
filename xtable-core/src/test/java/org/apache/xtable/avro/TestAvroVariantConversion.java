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
 
package org.apache.xtable.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.SchemaExtractorException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

/**
 * A Hudi variant column arrives as an Avro record with the {@code variant} logical type and the
 * {@code metadata}/{@code value} binary components. These tests pin how that record maps to {@link
 * InternalType#VARIANT} and back, at the top level and nested, and that look-alike records are left
 * alone.
 */
public class TestAvroVariantConversion {
  private static final AvroSchemaConverter CONVERTER = AvroSchemaConverter.getInstance();

  private static String variant(String name) {
    return "{\"type\":\"record\",\"name\":\""
        + name
        + "\",\"logicalType\":\"variant\",\"fields\":["
        + "{\"name\":\"metadata\",\"type\":\"bytes\",\"doc\":\"Variant metadata component\"},"
        + "{\"name\":\"value\",\"type\":\"bytes\",\"doc\":\"Variant value component\"}]}";
  }

  @Test
  void variantColumnsBecomeVariantTypeAtAnyDepth() {
    Schema avro =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"t\",\"fields\":["
                    + "{\"name\":\"v\",\"type\":[\"null\","
                    + variant("variant")
                    + "],\"default\":null},"
                    + "{\"name\":\"v_req\",\"type\":"
                    + variant("variant_req")
                    + "},"
                    + "{\"name\":\"s\",\"type\":{\"type\":\"record\",\"name\":\"s_type\",\"fields\":["
                    + "{\"name\":\"inner\",\"type\":"
                    + variant("inner_variant")
                    + "}]}},"
                    + "{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":"
                    + variant("list_variant")
                    + "}},"
                    + "{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":[\"null\","
                    + variant("map_variant")
                    + "]}}]}");

    InternalSchema internal = CONVERTER.toInternalSchema(avro);

    assertVariant(field(internal, "v").getSchema(), true);
    assertVariant(field(internal, "v_req").getSchema(), false);
    assertVariant(field(field(internal, "s").getSchema(), "inner").getSchema(), false);
    assertVariant(
        field(field(internal, "l").getSchema(), InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
            .getSchema(),
        false);
    assertVariant(
        field(field(internal, "m").getSchema(), InternalField.Constants.MAP_VALUE_FIELD_NAME)
            .getSchema(),
        true);

    // the Avro form written back carries the logical type and components again, and converting
    // it once more yields the same internal schema
    Schema back = CONVERTER.fromInternalSchema(internal);
    assertVariantAvro(back.getField("v").schema().getTypes().get(1));
    assertVariantAvro(back.getField("v_req").schema());
    assertVariantAvro(back.getField("s").schema().getField("inner").schema());
    assertVariantAvro(back.getField("l").schema().getElementType());
    assertVariantAvro(back.getField("m").schema().getValueType().getTypes().get(1));
    assertEquals(internal, CONVERTER.toInternalSchema(back));
  }

  @Test
  void hudiVariantSchemaIsRecognized() {
    Schema avro =
        SchemaBuilder.record("t")
            .fields()
            .name("v")
            .type(HoodieSchema.createVariant().toAvroSchema())
            .noDefault()
            .endRecord();

    InternalSchema internal = CONVERTER.toInternalSchema(avro);

    assertVariant(field(internal, "v").getSchema(), false);
    assertVariantAvro(CONVERTER.fromInternalSchema(internal).getField("v").schema());
  }

  @Test
  void ordinaryRecordWithMetadataAndValueStaysRecord() {
    Schema avro =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"v\",\"type\":"
                    + "{\"type\":\"record\",\"name\":\"pair\",\"fields\":["
                    + "{\"name\":\"metadata\",\"type\":\"bytes\"},"
                    + "{\"name\":\"value\",\"type\":\"bytes\"}]}}]}");

    InternalSchema internal = CONVERTER.toInternalSchema(avro);

    InternalSchema pair = field(internal, "v").getSchema();
    assertEquals(InternalType.RECORD, pair.getDataType());
    assertEquals(
        Arrays.asList(InternalType.BYTES, InternalType.BYTES),
        pair.getFields().stream()
            .map(f -> f.getSchema().getDataType())
            .collect(Collectors.toList()));
    Schema back = CONVERTER.fromInternalSchema(internal);
    assertNull(back.getField("v").schema().getProp("logicalType"));
  }

  @Test
  void shreddedVariantIsRejected() {
    Schema avro =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"v\",\"type\":"
                    + "{\"type\":\"record\",\"name\":\"variant\",\"logicalType\":\"variant\",\"fields\":["
                    + "{\"name\":\"metadata\",\"type\":\"bytes\"},"
                    + "{\"name\":\"value\",\"type\":[\"null\",\"bytes\"],\"default\":null},"
                    + "{\"name\":\"typed_value\",\"type\":[\"null\",\"int\"],\"default\":null}]}}]}");

    NotSupportedException ex =
        assertThrows(NotSupportedException.class, () -> CONVERTER.toInternalSchema(avro));
    assertTrue(ex.getMessage().contains("typed_value"), ex.getMessage());
  }

  @Test
  void variantWithUnexpectedComponentsIsRejected() {
    Schema avro =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"v\",\"type\":"
                    + "{\"type\":\"record\",\"name\":\"variant\",\"logicalType\":\"variant\",\"fields\":["
                    + "{\"name\":\"metadata\",\"type\":\"string\"},"
                    + "{\"name\":\"value\",\"type\":\"bytes\"}]}}]}");

    SchemaExtractorException ex =
        assertThrows(SchemaExtractorException.class, () -> CONVERTER.toInternalSchema(avro));
    assertTrue(ex.getMessage().contains("metadata"), ex.getMessage());
  }

  private static InternalField field(InternalSchema schema, String name) {
    return schema.getFields().stream()
        .filter(f -> f.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no field " + name + " in " + schema));
  }

  private static void assertVariant(InternalSchema schema, boolean nullable) {
    assertEquals(InternalType.VARIANT, schema.getDataType());
    assertEquals(nullable, schema.isNullable());
    assertNull(schema.getFields(), "a variant has no child fields of its own");
  }

  private static void assertVariantAvro(Schema schema) {
    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals("variant", schema.getProp("logicalType"));
    assertEquals(
        Arrays.asList("metadata", "value"),
        schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
    assertTrue(
        schema.getFields().stream().allMatch(f -> f.schema().getType() == Schema.Type.BYTES));
  }
}
