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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.AllArgsConstructor;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.util.ContentFileUtil;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.storage.FilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;

@AllArgsConstructor(staticName = "of")
public class IcebergDataFileUpdatesSync {
  private final IcebergColumnStatsConverter columnStatsConverter;
  private final IcebergPartitionValueConverter partitionValueConverter;

  public void applySnapshot(
      Table table,
      InternalTable internalTable,
      Transaction transaction,
      List<PartitionFileGroup> partitionedDataFiles,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {

    Map<String, DataFile> previousFiles = new HashMap<>();
    try (CloseableIterable<FileScanTask> iterator = table.newScan().planFiles()) {
      StreamSupport.stream(iterator.spliterator(), false)
          .map(FileScanTask::file)
          .forEach(file -> previousFiles.put(file.path().toString(), file));
    } catch (Exception e) {
      throw new ReadException("Failed to iterate through Iceberg data files", e);
    }

    FilesDiff<InternalFile, DataFile> diff =
        InternalFilesDiff.findNewAndRemovedFiles(partitionedDataFiles, previousFiles);

    applyDiff(
        transaction, diff.getFilesAdded(), diff.getFilesRemoved(), schema, partitionSpec, metadata);
  }

  public void applyDiff(
      Transaction transaction,
      InternalFilesDiff internalFilesDiff,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {

    Collection<DataFile> filesRemoved =
        internalFilesDiff.dataFilesRemoved().stream()
            .map(file -> getDataFile(partitionSpec, schema, file))
            .collect(Collectors.toList());

    applyDiff(
        transaction,
        internalFilesDiff.dataFilesAdded(),
        filesRemoved,
        schema,
        partitionSpec,
        metadata);
  }

  private void applyDiff(
      Transaction transaction,
      Collection<? extends InternalFile> filesAdded,
      Collection<DataFile> filesRemoved,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {
    OverwriteFiles overwriteFiles = transaction.newOverwrite();
    filesAdded.stream()
        .filter(InternalDataFile.class::isInstance)
        .map(file -> (InternalDataFile) file)
        .forEach(f -> overwriteFiles.addFile(getDataFile(partitionSpec, schema, f)));
    filesRemoved.forEach(overwriteFiles::deleteFile);
    overwriteFiles.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
    overwriteFiles.commit();
  }

  /**
   * Commits a files diff together with deletion vectors as one row delta, so that readers never see
   * the added rows without the deletes that belong to them.
   *
   * <p>The commit fails rather than merges when the table changed underneath it: it is validated
   * from the current snapshot, a file to remove that the table does not hold fails it, and so does
   * a deletion vector whose data file is neither live nor added by this commit. Iceberg drops the
   * deletion vector of every data file the diff removes.
   *
   * @param deletedPositionsByDataFile row positions deleted from data files, keyed by the path the
   *     data file is recorded under
   * @param partitionsOfDataFiles the partitions holding those data files; the lookup of their
   *     current deletion vectors reads only the manifests of these partitions
   * @param replaceDeletionVectors when true the positions are everything deleted from the file and
   *     replace its current deletion vector, an empty collection leaving the file without one; when
   *     false they are merged into the file's current deletion vector
   */
  public void applyRowDelta(
      Table table,
      Transaction transaction,
      InternalFilesDiff internalFilesDiff,
      Map<String, ? extends Collection<Long>> deletedPositionsByDataFile,
      Collection<List<PartitionValue>> partitionsOfDataFiles,
      boolean replaceDeletionVectors,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {
    RowDelta rowDelta = transaction.newRowDelta();
    Snapshot currentSnapshot = transaction.table().currentSnapshot();
    if (currentSnapshot != null) {
      // Without a starting snapshot Iceberg validates against the whole history, in which the
      // deletion vector this commit supersedes counts as concurrently added.
      rowDelta.validateFromSnapshot(currentSnapshot.snapshotId());
    }
    rowDelta.validateDeletedFiles();
    Map<String, DataFile> filesAdded = new HashMap<>();
    for (InternalDataFile file : internalFilesDiff.dataFilesAdded()) {
      DataFile dataFile = getDataFile(partitionSpec, schema, file);
      filesAdded.put(dataFile.location(), dataFile);
      rowDelta.addRows(dataFile);
    }
    Set<String> filesRemoved = new HashSet<>();
    for (InternalDataFile file : internalFilesDiff.dataFilesRemoved()) {
      DataFile dataFile = getDataFile(partitionSpec, schema, file);
      filesRemoved.add(dataFile.location());
      rowDelta.removeRows(dataFile);
    }
    if (!deletedPositionsByDataFile.isEmpty()) {
      addDeletionVectors(
          table,
          rowDelta,
          deletedPositionsByDataFile,
          partitionFilter(partitionSpec, schema, partitionsOfDataFiles),
          replaceDeletionVectors,
          filesAdded,
          filesRemoved,
          partitionSpec);
    }
    rowDelta.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
    rowDelta.commit();
  }

  private void addDeletionVectors(
      Table table,
      RowDelta rowDelta,
      Map<String, ? extends Collection<Long>> deletedPositionsByDataFile,
      Expression partitionFilter,
      boolean replaceDeletionVectors,
      Map<String, DataFile> filesAdded,
      Set<String> filesRemoved,
      PartitionSpec partitionSpec) {
    int formatVersion = ((HasTableOperations) table).operations().current().formatVersion();
    if (formatVersion < 3) {
      throw new NotSupportedException(
          String.format(
              "Deletion vectors need Iceberg format version 3 or later, but table %s is at version"
                  + " %d",
              table.name(), formatVersion));
    }
    Map<String, FileScanTask> liveFiles =
        liveFiles(table, deletedPositionsByDataFile.keySet(), filesAdded.keySet(), partitionFilter);
    List<String> liveFilesWithNewDeletionVectors = new ArrayList<>();
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DeleteWriteResult written;
    try (DVFileWriter writer =
        new BaseDVFileWriter(
            fileFactory,
            path ->
                replaceDeletionVectors
                    ? null
                    : currentDeletionVector(liveFiles.get(path))
                        .map(deletionVector -> readPositions(table, deletionVector))
                        .orElse(null))) {
      for (Map.Entry<String, ? extends Collection<Long>> entry :
          deletedPositionsByDataFile.entrySet()) {
        String path = entry.getKey();
        FileScanTask liveFile = liveFiles.get(path);
        DataFile addedFile = filesAdded.get(path);
        if (filesRemoved.contains(path) || (liveFile == null && addedFile == null)) {
          throw new UpdateException(
              "Deleted positions reference a data file that is not live in the Iceberg table: "
                  + path);
        }
        if (liveFile != null) {
          liveFilesWithNewDeletionVectors.add(path);
          if (replaceDeletionVectors) {
            currentDeletionVector(liveFile).ifPresent(rowDelta::removeDeletes);
          }
        }
        PartitionSpec spec = liveFile != null ? liveFile.spec() : partitionSpec;
        StructLike partition =
            liveFile != null ? liveFile.file().partition() : addedFile.partition();
        for (Long position : entry.getValue()) {
          writer.delete(path, position, spec, partition);
        }
      }
      writer.close();
      written = writer.result();
    } catch (IOException e) {
      throw new UpdateException("Failed to write Iceberg deletion vectors", e);
    }
    written.deleteFiles().forEach(rowDelta::addDeletes);
    written.rewrittenDeleteFiles().forEach(rowDelta::removeDeletes);
    rowDelta.validateDataFilesExist(liveFilesWithNewDeletionVectors);
  }

  /**
   * The live data files among the given paths, with the delete files Iceberg applies to them. The
   * scan is limited to the partition filter; a path that it does not find and that the commit does
   * not add is looked up in the whole table, so the partitions change the cost but never the
   * result.
   */
  private static Map<String, FileScanTask> liveFiles(
      Table table, Set<String> paths, Set<String> filesAdded, Expression partitionFilter) {
    Map<String, FileScanTask> liveFiles = scanLiveFiles(table, paths, partitionFilter);
    if (partitionFilter.op() != Expression.Operation.TRUE
        && paths.stream()
            .anyMatch(path -> !liveFiles.containsKey(path) && !filesAdded.contains(path))) {
      return scanLiveFiles(table, paths, Expressions.alwaysTrue());
    }
    return liveFiles;
  }

  private static Map<String, FileScanTask> scanLiveFiles(
      Table table, Set<String> paths, Expression partitionFilter) {
    Map<String, FileScanTask> liveFiles = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().filter(partitionFilter).planFiles()) {
      for (FileScanTask task : tasks) {
        if (paths.contains(task.file().location())) {
          liveFiles.put(task.file().location(), task);
        }
      }
    } catch (IOException e) {
      throw new ReadException("Failed to iterate through Iceberg data files", e);
    }
    return liveFiles;
  }

  /**
   * A row filter that matches exactly the rows of the given partitions of the spec, which a scan
   * projects onto the partition summaries of each manifest to skip the manifests of other
   * partitions. Without a partitioned spec or partitions it matches everything.
   */
  private Expression partitionFilter(
      PartitionSpec spec, Schema schema, Collection<List<PartitionValue>> partitions) {
    if (!spec.isPartitioned() || partitions.isEmpty()) {
      return Expressions.alwaysTrue();
    }
    Expression filter = Expressions.alwaysFalse();
    for (List<PartitionValue> partitionValues : partitions) {
      StructLike partition = partitionValueConverter.toIceberg(spec, schema, partitionValues);
      if (partition == null) {
        return Expressions.alwaysTrue();
      }
      Expression partitionMatch = Expressions.alwaysTrue();
      for (int i = 0; i < spec.fields().size(); i++) {
        PartitionField field = spec.fields().get(i);
        if (field.transform().isVoid()) {
          continue;
        }
        UnboundTerm<Object> term = partitionTerm(schema.findColumnName(field.sourceId()), field);
        Object value = partition.get(i, Object.class);
        partitionMatch =
            Expressions.and(
                partitionMatch,
                value == null ? Expressions.isNull(term) : Expressions.equal(term, value));
      }
      filter = Expressions.or(filter, partitionMatch);
    }
    return filter;
  }

  @SuppressWarnings("unchecked")
  private static UnboundTerm<Object> partitionTerm(String sourceColumn, PartitionField field) {
    return field.transform().isIdentity()
        ? Expressions.ref(sourceColumn)
        : Expressions.transform(sourceColumn, (Transform<?, Object>) field.transform());
  }

  /**
   * The deletion vector Iceberg applies to a live data file, if it has one.
   *
   * @throws ReadException when the file has more than one delete file, or one that is not a
   *     deletion vector, since neither can be superseded by writing a deletion vector
   */
  private static Optional<DeleteFile> currentDeletionVector(FileScanTask liveFile) {
    if (liveFile == null || liveFile.deletes().isEmpty()) {
      return Optional.empty();
    }
    if (liveFile.deletes().size() > 1 || !ContentFileUtil.isDV(liveFile.deletes().get(0))) {
      throw new ReadException(
          "Expected at most one deletion vector for data file "
              + liveFile.file().location()
              + " but found "
              + liveFile.deletes());
    }
    return Optional.of(liveFile.deletes().get(0));
  }

  private static PositionDeleteIndex readPositions(Table table, DeleteFile deletionVector) {
    try (SeekableInputStream in = table.io().newInputFile(deletionVector.location()).newStream()) {
      in.seek(deletionVector.contentOffset());
      byte[] bytes = new byte[Math.toIntExact(deletionVector.contentSizeInBytes())];
      IOUtil.readFully(in, bytes, 0, bytes.length);
      return PositionDeleteIndex.deserialize(bytes, deletionVector);
    } catch (IOException e) {
      throw new ReadException(
          "Failed to read the deletion vector at " + deletionVector.location(), e);
    }
  }

  private DataFile getDataFile(
      PartitionSpec partitionSpec, Schema schema, InternalDataFile dataFile) {
    DataFiles.Builder builder =
        DataFiles.builder(partitionSpec)
            .withPath(dataFile.getPhysicalPath())
            .withFileSizeInBytes(dataFile.getFileSizeBytes())
            .withMetrics(
                columnStatsConverter.toIceberg(
                    schema, dataFile.getRecordCount(), dataFile.getColumnStats()))
            .withFormat(convertFileFormat(dataFile.getFileFormat()));
    if (partitionSpec.isPartitioned()) {
      builder.withPartition(
          partitionValueConverter.toIceberg(partitionSpec, schema, dataFile.getPartitionValues()));
    }
    return builder.build();
  }

  private static FileFormat convertFileFormat(
      org.apache.xtable.model.storage.FileFormat fileFormat) {
    switch (fileFormat) {
      case APACHE_PARQUET:
        return FileFormat.PARQUET;
      case APACHE_ORC:
        return FileFormat.ORC;
      case APACHE_AVRO:
        return FileFormat.AVRO;
      default:
        throw new NotSupportedException(
            "Conversion to Iceberg with file format: " + fileFormat.name() + " is not supported");
    }
  }
}
