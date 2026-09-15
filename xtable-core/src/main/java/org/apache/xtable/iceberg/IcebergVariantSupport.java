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

import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import org.apache.xtable.exception.NotSupportedException;

/**
 * The rules of the Iceberg table specification for variant columns that XTable enforces before it
 * writes metadata, so that a table never ends up with metadata Iceberg readers reject.
 */
final class IcebergVariantSupport {
  /** Variant is defined by the v3 table specification and invalid in earlier versions. */
  static final int MIN_FORMAT_VERSION = 3;

  private IcebergVariantSupport() {}

  /**
   * Full paths of every variant column in the schema, including variants nested in structs, list
   * elements and map keys or values. Empty when the schema has no variant.
   */
  static List<String> variantColumns(Schema schema) {
    List<String> columns = new ArrayList<>();
    collectVariantColumns(schema.asStruct(), null, columns);
    return columns;
  }

  /**
   * Fails when the schema has a variant column but the table's format version cannot hold one.
   *
   * @param schema the Iceberg schema about to be written
   * @param formatVersion the format version the schema would be written under
   * @param versionDescription how the caller arrived at that version, for the error message
   */
  static void requireFormatVersion(Schema schema, int formatVersion, String versionDescription) {
    if (formatVersion >= MIN_FORMAT_VERSION) {
      return;
    }
    List<String> variants = variantColumns(schema);
    if (!variants.isEmpty()) {
      throw new NotSupportedException(
          String.format(
              "Variant columns %s require Iceberg format version %d or higher, but %s",
              variants, MIN_FORMAT_VERSION, versionDescription));
    }
  }

  private static void collectVariantColumns(Type type, String path, List<String> columns) {
    switch (type.typeId()) {
      case VARIANT:
        columns.add(path);
        break;
      case STRUCT:
        for (Types.NestedField field : type.asStructType().fields()) {
          collectVariantColumns(field.type(), childPath(path, field.name()), columns);
        }
        break;
      case LIST:
        collectVariantColumns(type.asListType().elementType(), childPath(path, "element"), columns);
        break;
      case MAP:
        collectVariantColumns(type.asMapType().keyType(), childPath(path, "key"), columns);
        collectVariantColumns(type.asMapType().valueType(), childPath(path, "value"), columns);
        break;
      default:
        break;
    }
  }

  private static String childPath(String parent, String name) {
    return parent == null ? name : parent + "." + name;
  }
}
