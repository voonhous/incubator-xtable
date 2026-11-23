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

import static org.apache.xtable.MergeOnReadAssertions.assertIcebergMatchesHudi;
import static org.apache.xtable.MergeOnReadAssertions.deletedKeysByDataFileInIceberg;
import static org.apache.xtable.MergeOnReadAssertions.deletedRowCount;
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorTable;
import static org.apache.xtable.MergeOnReadAssertions.icebergRows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;

/**
 * The merge-on-read counterpart of {@link ITIcebergPlainBasePath}: at a base path without a scheme
 * the deletion vectors of a deltacommit have to name their data files the way those files were
 * registered, or Iceberg refuses them as deletes of a data file it does not know.
 */
class ITIcebergPlainBasePathMergeOnRead {
  @TempDir public static Path tempDir;

  private static SparkSession spark;

  @BeforeAll
  static void setupOnce() {
    spark = MergeOnReadAssertions.sparkSession(tempDir);
  }

  @AfterAll
  static void teardown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void deletionVectorsFindTheirDataFilesAtAPlainBasePath() {
    String basePath = tempDir.resolve("plain_mor_" + UUID.randomUUID()).toString();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchemaAtBasePath(
            "plain_base_path_mor",
            basePath,
            null,
            HoodieTableType.MERGE_ON_READ,
            deletionVectorTable(8))) {
      assertFalse(
          table.getBasePath().contains(":"),
          "the base path must reach Hudi without a scheme: " + table.getBasePath());
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      assertIcebergMatchesHudi(spark, basePath);

      table.upsertRecords(inserts.subList(0, 10), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(10, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(50, icebergRows(spark, basePath).size());

      table.deleteRecords(inserts.subList(10, 15), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(15, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(45, icebergRows(spark, basePath).size());
    }
  }
}
