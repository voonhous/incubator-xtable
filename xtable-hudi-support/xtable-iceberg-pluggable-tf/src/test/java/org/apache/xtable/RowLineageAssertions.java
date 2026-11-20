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

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

import org.apache.xtable.timeline.IcebergSnapshotInstants;

/**
 * Reads the row lineage of every live row of the Iceberg table the way Iceberg engines do, and
 * checks it across a Hudi operation against what the Hudi meta fields say the operation did to each
 * row.
 */
final class RowLineageAssertions {
  private RowLineageAssertions() {}

  /**
   * The row lineage of one row as Iceberg reads it, with the Hudi meta fields of the row and the
   * Iceberg data file it is read from.
   */
  static final class RowLineage {
    final String commitTime;
    final String commitSeqno;
    final String dataFile;
    final long rowId;
    final long lastUpdatedSequenceNumber;

    RowLineage(
        String commitTime,
        String commitSeqno,
        String dataFile,
        long rowId,
        long lastUpdatedSequenceNumber) {
      this.commitTime = commitTime;
      this.commitSeqno = commitSeqno;
      this.dataFile = dataFile;
      this.rowId = rowId;
      this.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof RowLineage)) {
        return false;
      }
      RowLineage that = (RowLineage) other;
      return commitTime.equals(that.commitTime)
          && commitSeqno.equals(that.commitSeqno)
          && dataFile.equals(that.dataFile)
          && rowId == that.rowId
          && lastUpdatedSequenceNumber == that.lastUpdatedSequenceNumber;
    }

    @Override
    public int hashCode() {
      return Objects.hash(commitTime, commitSeqno, dataFile, rowId, lastUpdatedSequenceNumber);
    }

    @Override
    public String toString() {
      return String.format(
          "%s/%s in %s: _row_id=%d, _last_updated_sequence_number=%d",
          commitTime, commitSeqno, dataFile, rowId, lastUpdatedSequenceNumber);
    }
  }

  /** How the rows of the table changed across an operation. */
  static final class Changes {
    int unchanged;
    int unchangedMovedToAnotherFile;
    int changedOrNew;
  }

  /**
   * The row lineage of every live row by record key, through Spark on Iceberg and through Iceberg's
   * Parquet reader with the table's name mapping, which have to agree.
   */
  static Map<String, RowLineage> rowLineage(SparkSession spark, String basePath) {
    Map<String, RowLineage> throughSpark = rowLineageThroughSpark(spark, basePath);
    assertEquals(
        throughSpark,
        rowLineageThroughIcebergReader(basePath),
        "Spark and Iceberg's Parquet reader read different row lineage");
    return throughSpark;
  }

  private static Map<String, RowLineage> rowLineageThroughSpark(
      SparkSession spark, String basePath) {
    List<Row> rows =
        spark
            .read()
            .format("iceberg")
            .load(basePath)
            .selectExpr(
                HoodieRecord.RECORD_KEY_METADATA_FIELD,
                HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                MetadataColumns.FILE_PATH.name(),
                MetadataColumns.ROW_ID.name(),
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())
            .collectAsList();
    Map<String, RowLineage> lineage = new TreeMap<>();
    for (Row row : rows) {
      RowLineage previous =
          lineage.put(
              row.getString(0),
              new RowLineage(
                  row.getString(1),
                  row.getString(2),
                  row.getString(3),
                  row.getLong(4),
                  row.getLong(5)));
      assertEquals(null, previous, "duplicate record key " + row.getString(0));
    }
    return lineage;
  }

  /**
   * Reads every planned data file with Iceberg's generic Parquet readers, applying the table's name
   * mapping and the file's deletion vector. Iceberg's {@code IcebergGenerics} does not apply the
   * name mapping, so it cannot see the values a file without field ids stores.
   */
  @SneakyThrows
  private static Map<String, RowLineage> rowLineageThroughIcebergReader(String basePath) {
    Table table = icebergTable(basePath);
    NameMapping nameMapping =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    Schema projection =
        TypeUtil.join(
            MetadataColumns.schemaWithRowLineage(
                table
                    .schema()
                    .select(
                        HoodieRecord.RECORD_KEY_METADATA_FIELD,
                        HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                        HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)),
            new Schema(MetadataColumns.ROW_POSITION));
    BaseDeleteLoader deleteLoader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.location()));
    Map<String, RowLineage> lineage = new TreeMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String dataFile = task.file().location();
        PositionDeleteIndex deleted =
            task.deletes().isEmpty()
                ? null
                : deleteLoader.loadPositionDeletes(task.deletes(), dataFile);
        Map<Integer, ?> constants =
            PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);
        try (CloseableIterable<Record> records =
            Parquet.read(table.io().newInputFile(dataFile))
                .project(projection)
                .withNameMapping(nameMapping)
                .createReaderFunc(
                    fileSchema ->
                        GenericParquetReaders.buildReader(projection, fileSchema, constants))
                .build()) {
          for (Record record : records) {
            long position = (Long) record.getField(MetadataColumns.ROW_POSITION.name());
            if (deleted != null && deleted.isDeleted(position)) {
              continue;
            }
            String key = (String) record.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD);
            RowLineage previous =
                lineage.put(
                    key,
                    new RowLineage(
                        (String) record.getField(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
                        (String) record.getField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD),
                        dataFile,
                        (Long) record.getField(MetadataColumns.ROW_ID.name()),
                        (Long)
                            record.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())));
            assertEquals(null, previous, "duplicate record key " + key);
          }
        }
      }
    }
    return lineage;
  }

  /**
   * Adds the sequence number of the snapshot that recorded each Hudi commit still in the Iceberg
   * table's history. Called after every operation, so a commit stays known after archival expires
   * its snapshot.
   */
  static void recordSequenceNumbers(String basePath, Map<String, Long> sequenceNumberByCommit) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(getStorageConf(new Configuration()))
            .setBasePath(basePath)
            .build();
    for (Snapshot snapshot : icebergTable(basePath).snapshots()) {
      HoodieInstant instant =
          IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator());
      if (!HoodieTimeline.SAVEPOINT_ACTION.equals(instant.getAction())) {
        sequenceNumberByCommit.putIfAbsent(instant.requestedTime(), snapshot.sequenceNumber());
      }
    }
  }

  /**
   * Checks the row lineage after an operation against the row lineage before it and the Hudi meta
   * fields. Row ids are unique. A row keeps its row id exactly when it keeps its {@code
   * _hoodie_commit_seqno}, and then keeps its last updated sequence number too; any other row has a
   * row id no row had before. Unless {@code sequenceNumberByCommit} is null, every row's last
   * updated sequence number is the sequence number of the snapshot that recorded its {@code
   * _hoodie_commit_time}.
   */
  static Changes assertRowLineage(
      Map<String, RowLineage> before,
      Map<String, RowLineage> after,
      Map<String, Long> sequenceNumberByCommit) {
    assertUniqueRowIds(after);
    Set<Long> rowIdsBefore =
        before.values().stream().map(lineage -> lineage.rowId).collect(Collectors.toSet());
    Changes changes = new Changes();
    after.forEach(
        (key, lineage) -> {
          if (sequenceNumberByCommit != null) {
            Long recordedBy = sequenceNumberByCommit.get(lineage.commitTime);
            assertNotNull(recordedBy, "no snapshot recorded commit " + lineage.commitTime);
            assertEquals(
                recordedBy.longValue(),
                lineage.lastUpdatedSequenceNumber,
                "last updated sequence number of " + key + ": " + lineage);
          }
          RowLineage previous = before.get(key);
          if (previous != null && previous.commitSeqno.equals(lineage.commitSeqno)) {
            assertEquals(previous.rowId, lineage.rowId, "row id of unchanged row " + key);
            assertEquals(
                previous.lastUpdatedSequenceNumber,
                lineage.lastUpdatedSequenceNumber,
                "last updated sequence number of unchanged row " + key);
            changes.unchanged++;
            if (!previous.dataFile.equals(lineage.dataFile)) {
              changes.unchangedMovedToAnotherFile++;
            }
          } else {
            assertFalse(
                rowIdsBefore.contains(lineage.rowId),
                "changed or new row " + key + " reuses a row id: " + lineage);
            changes.changedOrNew++;
          }
        });
    return changes;
  }

  static void assertUniqueRowIds(Map<String, RowLineage> lineage) {
    Map<Long, String> keyByRowId = new HashMap<>();
    lineage.forEach(
        (key, rowLineage) -> {
          String other = keyByRowId.put(rowLineage.rowId, key);
          assertEquals(
              null, other, "row id " + rowLineage.rowId + " of both " + other + " and " + key);
        });
  }

  /** The top-level column names of a Parquet file, from its footer. */
  static List<String> parquetColumns(HoodieTableMetaClient metaClient, String path) {
    return ParquetUtils.readMetadata(metaClient.getStorage(), new StoragePath(path))
        .getFileMetaData()
        .getSchema()
        .getFields()
        .stream()
        .map(Type::getName)
        .collect(Collectors.toList());
  }
}
