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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;

/**
 * A files diff with deleted positions is committed as one row delta that either lands whole or
 * fails: it never leaves a deletion vector on a file that is not live, and never ignores a file it
 * was asked to remove.
 */
class TestIcebergDeletionVectorSync {
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @TempDir static Path tempDir;

  private final IcebergDataFileUpdatesSync sync =
      IcebergDataFileUpdatesSync.of(
          IcebergColumnStatsConverter.getInstance(), IcebergPartitionValueConverter.getInstance());

  private static Table newTable(int formatVersion) {
    return new HadoopTables(new Configuration())
        .create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            Collections.singletonMap(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tempDir.resolve("table_" + UUID.randomUUID()).toUri().toString());
  }

  private static InternalDataFile dataFile(Table table, String name) {
    return InternalDataFile.builder()
        .physicalPath(table.location() + "/" + name + ".parquet")
        .fileFormat(FileFormat.APACHE_PARQUET)
        .fileSizeBytes(100)
        .recordCount(10)
        .partitionValues(Collections.emptyList())
        .columnStats(Collections.emptyList())
        .build();
  }

  private void apply(
      Table table,
      List<InternalDataFile> added,
      List<InternalDataFile> removed,
      Map<String, List<Long>> deletedPositions,
      boolean replaceDeletionVectors) {
    Transaction transaction = table.newTransaction();
    sync.applyRowDelta(
        table,
        transaction,
        InternalFilesDiff.builder().filesAdded(added).filesRemoved(removed).build(),
        deletedPositions,
        replaceDeletionVectors,
        transaction.table().schema(),
        transaction.table().spec(),
        TableSyncMetadata.of(Instant.now(), Collections.emptyList()));
    transaction.commitTransaction();
    table.refresh();
  }

  /** The positions deleted from each live data file, by file name, as an Iceberg reader loads. */
  @SneakyThrows
  private static Map<String, Set<Long>> deletedPositions(Table table) {
    Map<String, Set<Long>> deleted = new TreeMap<>();
    BaseDeleteLoader loader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.location()));
    int deletionVectors = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String location = task.file().location();
        Set<Long> positions = new TreeSet<>();
        deletionVectors += task.deletes().size();
        if (!task.deletes().isEmpty()) {
          loader.loadPositionDeletes(task.deletes(), location).forEach(positions::add);
        }
        String fileName = location.substring(location.lastIndexOf('/') + 1);
        deleted.put(fileName.substring(0, fileName.indexOf('.')), positions);
      }
    }
    String recorded =
        table.currentSnapshot() == null
            ? "0"
            : table.currentSnapshot().summary().getOrDefault("total-delete-files", "0");
    assertEquals(String.valueOf(deletionVectors), recorded, "deletion vectors of files not live");
    return deleted;
  }

  private static Map<String, Set<Long>> expected(Object... fileThenPositions) {
    Map<String, Set<Long>> expected = new TreeMap<>();
    for (int i = 0; i < fileThenPositions.length; i += 2) {
      @SuppressWarnings("unchecked")
      List<Long> positions = (List<Long>) fileThenPositions[i + 1];
      expected.put((String) fileThenPositions[i], new TreeSet<>(positions));
    }
    return expected;
  }

  private static Map<String, List<Long>> positions(InternalDataFile file, Long... positions) {
    return Collections.singletonMap(file.getPhysicalPath(), Arrays.asList(positions));
  }

  @Test
  void positionsAreMergedIntoTheCurrentDeletionVector() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a, b), Collections.emptyList(), Collections.emptyMap(), false);
    assertEquals(expected("a", Arrays.asList(), "b", Arrays.asList()), deletedPositions(table));

    apply(table, Collections.emptyList(), Collections.emptyList(), positions(a, 0L, 3L), false);
    assertEquals(
        expected("a", Arrays.asList(0L, 3L), "b", Arrays.asList()), deletedPositions(table));

    apply(table, Collections.emptyList(), Collections.emptyList(), positions(a, 3L, 7L), false);
    assertEquals(
        expected("a", Arrays.asList(0L, 3L, 7L), "b", Arrays.asList()), deletedPositions(table));
  }

  @Test
  void positionsReplaceTheCurrentDeletionVector() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), positions(a, 0L, 3L), false);
    assertEquals(expected("a", Arrays.asList(0L, 3L)), deletedPositions(table));

    apply(table, Collections.emptyList(), Collections.emptyList(), positions(a, 5L), true);
    assertEquals(expected("a", Arrays.asList(5L)), deletedPositions(table));

    apply(table, Collections.emptyList(), Collections.emptyList(), positions(a), true);
    assertEquals(expected("a", Arrays.asList()), deletedPositions(table));
  }

  @Test
  void removingADataFileRemovesItsDeletionVector() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a), Collections.emptyList(), positions(a, 1L), false);

    // The replacement carries its own deletes in the commit that adds it.
    apply(table, Arrays.asList(b), Arrays.asList(a), positions(b, 2L), true);
    assertEquals(expected("b", Arrays.asList(2L)), deletedPositions(table));
  }

  @Test
  void positionsOfAFileThatIsNotLiveFailTheCommit() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile unknown = dataFile(table, "unknown");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);
    long snapshot = table.currentSnapshot().snapshotId();

    UpdateException notLive =
        assertThrows(
            UpdateException.class,
            () ->
                apply(
                    table,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    positions(unknown, 1L),
                    false));
    assertTrue(notLive.getMessage().contains("unknown.parquet"), notLive.getMessage());
    assertThrows(
        UpdateException.class,
        () -> apply(table, Collections.emptyList(), Arrays.asList(a), positions(a, 1L), false));
    table.refresh();
    assertEquals(snapshot, table.currentSnapshot().snapshotId());
  }

  @Test
  void removingAFileTheTableDoesNotHoldFailsTheCommit() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);
    long snapshot = table.currentSnapshot().snapshotId();

    ValidationException missing =
        assertThrows(
            ValidationException.class,
            () ->
                apply(
                    table,
                    Collections.emptyList(),
                    Arrays.asList(dataFile(table, "unknown")),
                    Collections.emptyMap(),
                    false));
    assertTrue(missing.getMessage().contains("unknown.parquet"), missing.getMessage());
    table.refresh();
    assertEquals(snapshot, table.currentSnapshot().snapshotId());
  }

  @Test
  void aDataFileRemovedSinceTheDeletesWereReadFailsTheCommit() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);
    Table concurrent = new HadoopTables(new Configuration()).load(table.location());

    Transaction transaction = table.newTransaction();
    sync.applyRowDelta(
        table,
        transaction,
        InternalFilesDiff.builder()
            .filesAdded(Collections.emptyList())
            .filesRemoved(Collections.emptyList())
            .build(),
        positions(a, 1L),
        false,
        transaction.table().schema(),
        transaction.table().spec(),
        TableSyncMetadata.of(Instant.now(), Collections.emptyList()));
    apply(concurrent, Collections.emptyList(), Arrays.asList(a), Collections.emptyMap(), false);

    ValidationException missing =
        assertThrows(ValidationException.class, transaction::commitTransaction);
    assertTrue(missing.getMessage().contains("missing data files"), missing.getMessage());
    table.refresh();
    assertEquals(Collections.emptyMap(), deletedPositions(table));
  }

  @Test
  void deletionVectorsAreRefusedBelowFormatVersionThree() {
    Table table = newTable(2);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);

    NotSupportedException refused =
        assertThrows(
            NotSupportedException.class,
            () ->
                apply(
                    table,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    positions(a, 1L),
                    false));
    assertTrue(refused.getMessage().contains("format version 3"), refused.getMessage());
  }
}
