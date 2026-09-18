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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import org.apache.xtable.hudi.HudiTestUtil;

/**
 * A writer may hand Hudi a base path without a scheme ({@code /warehouse/t}), as a Spark job given
 * a bare path does. Hudi then spells a file one way in commit metadata ({@code /warehouse/t/f}) and
 * another way in file listings ({@code file:/warehouse/t/f}). Iceberg matches data files by path
 * string, so replaced files used to stay in the Iceberg snapshot and readers saw stale duplicates.
 * These tests pin that updates and deletes replace files in Iceberg, and that the deletion vectors
 * of the merge-on-read model find their data files, under such a base path.
 */
class ITIcebergSchemeLessBasePath {

  @TempDir public static Path tempDir;

  private static SparkSession sparkSession;

  @BeforeAll
  static void setupOnce() {
    sparkSession = SparkSession.builder().config(HudiTestUtil.getSparkConf(tempDir)).getOrCreate();
  }

  @AfterAll
  static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  private static Properties copyOnWriteProperties() {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.TABLE_FORMAT.key(), "ICEBERG");
    properties.put(
        HoodieTableConfig.VERSION.key(), String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
    properties.put(HoodieMetadataConfig.ENABLE.key(), "false");
    return properties;
  }

  private static Properties deletionVectorProperties() {
    Properties properties = copyOnWriteProperties();
    properties.put("hoodie.write.updates.as.deletes.and.inserts", "true");
    properties.put("hoodie.index.type", "SIMPLE");
    properties.put("xtable.iceberg.format-version", "3");
    return properties;
  }

  @Test
  void copyOnWriteUpdatesAndDeletesReplaceIcebergDataFiles() throws Exception {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchemaWithSchemeLessBasePath(
            "cow_scheme_less",
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            copyOnWriteProperties())) {
      assertFalse(
          table.getBasePath().contains(":"),
          "the base path must reach Hudi without a scheme: " + table.getBasePath());

      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      Table icebergTable = new HadoopTables(new Configuration()).load(table.getBasePath());
      assertEquals(50, readKeys(icebergTable).size());

      table.upsertRecords(inserts.subList(0, 10), true);
      icebergTable.refresh();
      assertTrue(
          deletedDataFiles(icebergTable.currentSnapshot()) > 0,
          "the upsert must remove the base files it rewrote from Iceberg");
      assertEquals(50, readKeys(icebergTable).size());

      table.deleteRecords(inserts.subList(10, 15), true);
      icebergTable.refresh();
      assertTrue(deletedDataFiles(icebergTable.currentSnapshot()) > 0);
      Set<String> keys = readKeys(icebergTable);
      assertEquals(45, keys.size());
      inserts.subList(10, 15).stream()
          .map(HoodieRecord::getRecordKey)
          .forEach(key -> assertFalse(keys.contains(key), "deleted key still visible: " + key));
      assertDataFilesQualified(icebergTable);
    }
  }

  @Test
  void mergeOnReadDeletionVectorsFindTheirDataFiles() throws Exception {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchemaWithSchemeLessBasePath(
            "mor_scheme_less",
            tempDir,
            null,
            HoodieTableType.MERGE_ON_READ,
            deletionVectorProperties())) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      Table icebergTable = new HadoopTables(new Configuration()).load(table.getBasePath());

      table.upsertRecords(inserts.subList(0, 10), true);
      icebergTable.refresh();
      assertTrue(
          Long.parseLong(
                  icebergTable
                      .currentSnapshot()
                      .summary()
                      .getOrDefault(SnapshotSummary.ADDED_DELETE_FILES_PROP, "0"))
              > 0,
          "the update must be recorded as deletion vectors");
      assertEquals(50, readKeys(icebergTable).size());

      table.deleteRecords(inserts.subList(10, 15), true);
      icebergTable.refresh();
      assertEquals(45, readKeys(icebergTable).size());
      assertDataFilesQualified(icebergTable);
    }
  }

  private static long deletedDataFiles(Snapshot snapshot) {
    return Long.parseLong(snapshot.summary().getOrDefault(SnapshotSummary.DELETED_FILES_PROP, "0"));
  }

  private static void assertDataFilesQualified(Table icebergTable) throws Exception {
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        assertTrue(
            file.location().startsWith("file:/"),
            "data files are registered with their qualified path: " + file.location());
      }
    }
  }

  private Set<String> readKeys(Table icebergTable) {
    List<Row> rows =
        sparkSession
            .read()
            .format("iceberg")
            .load(icebergTable.location())
            .select("key")
            .collectAsList();
    Set<String> keys = new HashSet<>();
    rows.forEach(row -> keys.add(row.getString(0)));
    assertEquals(rows.size(), keys.size(), "Iceberg reads must not expose duplicate keys");
    return keys;
  }
}
