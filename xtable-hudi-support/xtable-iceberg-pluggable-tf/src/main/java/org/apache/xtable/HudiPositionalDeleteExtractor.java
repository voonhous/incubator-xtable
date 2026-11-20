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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;

/**
 * Reads the row positions that the log files of a merge-on-read table delete from its base files,
 * which is what the Iceberg table records as deletion vectors. The log files are read through
 * Hudi's log reader, so inline log files and native delete files are handled alike.
 *
 * <p>Iceberg sees base files only, so a log file may hold nothing but deletes that carry the
 * position of each deleted row; anything else would be invisible to Iceberg readers and is refused.
 */
class HudiPositionalDeleteExtractor {
  static final String UPDATES_AS_DELETES_AND_INSERTS =
      "hoodie.write.updates.as.deletes.and.inserts";

  private final HoodieTableMetaClient metaClient;
  private final FileSystemViewManager viewManager;
  private SyncableFileSystemView fsView;
  private HoodieSchema tableSchema;

  HudiPositionalDeleteExtractor(
      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    this.metaClient = metaClient;
    this.viewManager = viewManager;
  }

  /**
   * The positions the log files written by a deltacommit delete, keyed by the path of the base file
   * they are deleted from, which is the latest base file of the log file's file group.
   */
  Map<String, Set<Long>> positionsDeletedBy(
      HoodieCommitMetadata commitMetadata, HoodieInstant deltacommit) {
    Map<String, Set<Long>> positionsByBaseFile = new LinkedHashMap<>();
    logFilesByFileGroup(commitMetadata, deltacommit)
        .forEach(
            (fileGroup, logFiles) -> {
              HoodieBaseFile baseFile =
                  fsView()
                      .getLatestBaseFile(fileGroup.getPartitionPath(), fileGroup.getFileId())
                      .orElseThrow(
                          () ->
                              new NotSupportedException(
                                  unsupportedLogContent(
                                      new HoodieLogFile(logFiles.iterator().next()),
                                      "records of a file group without a base file")));
              Set<Long> positions =
                  positionsByBaseFile.computeIfAbsent(
                      qualifiedPath(baseFile), path -> new LinkedHashSet<>());
              logFiles.forEach(
                  logFile ->
                      collectDeletedPositions(
                          fileGroup, baseFile, new HoodieLogFile(logFile), positions));
            });
    return positionsByBaseFile;
  }

  /**
   * Everything the remaining log files delete from the latest base file of each file group a
   * rollback changed, keyed by that base file's path. Those are the file groups the rollback
   * deleted files from, and the ones whose base files Iceberg removed when it recorded the
   * rolled-back commits, which the rollback makes live again. A file group left without a base file
   * is left out; one whose log files delete nothing maps to no positions.
   *
   * @param publishedCommitTimes requested times of the commits Iceberg has recorded; files of any
   *     other commit never reached Iceberg, so deleting them changes nothing there
   * @param filesRemovedByRolledBackCommits paths of the base files Iceberg removed when it recorded
   *     the rolled-back commits
   */
  Map<String, Set<Long>> positionsDeletedAfter(
      HoodieRollbackMetadata rollbackMetadata,
      Set<String> publishedCommitTimes,
      Collection<String> filesRemovedByRolledBackCommits) {
    Set<HoodieFileGroupId> fileGroups = new LinkedHashSet<>();
    rollbackMetadata
        .getPartitionMetadata()
        .forEach(
            (partitionPath, partitionMetadata) -> {
              for (String deleted : partitionMetadata.getSuccessDeleteFiles()) {
                StoragePath path = new StoragePath(deleted);
                if (FSUtils.isLogFile(path)) {
                  HoodieLogFile logFile = new HoodieLogFile(path);
                  if (publishedCommitTimes.contains(logFile.getDeltaCommitTime())) {
                    fileGroups.add(new HoodieFileGroupId(partitionPath, logFile.getFileId()));
                  }
                } else if (FSUtils.isBaseFile(path)
                    && publishedCommitTimes.contains(FSUtils.getCommitTimeWithFullPath(deleted))) {
                  fileGroups.add(
                      new HoodieFileGroupId(partitionPath, FSUtils.getFileIdFromFilePath(path)));
                }
              }
            });
    for (String removed : filesRemovedByRolledBackCommits) {
      StoragePath path = new StoragePath(removed);
      fileGroups.add(
          new HoodieFileGroupId(
              FSUtils.getRelativePartitionPath(metaClient.getBasePath(), path.getParent()),
              FSUtils.getFileIdFromFilePath(path)));
    }
    Map<String, Set<Long>> positionsByBaseFile = new LinkedHashMap<>();
    if (fileGroups.isEmpty()) {
      return positionsByBaseFile;
    }
    Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().lastInstant();
    if (!lastInstant.isPresent()) {
      return positionsByBaseFile;
    }
    fileGroups.stream()
        .map(HoodieFileGroupId::getPartitionPath)
        .distinct()
        .forEach(
            partitionPath ->
                // The merged slice is what a snapshot reader sees: the latest base file with every
                // committed log file, including those written after a compaction was scheduled.
                fsView()
                    .getLatestMergedFileSlicesBeforeOrOn(
                        partitionPath, lastInstant.get().requestedTime())
                    .filter(slice -> fileGroups.contains(slice.getFileGroupId()))
                    .filter(slice -> slice.getBaseFile().isPresent())
                    .collect(Collectors.toList())
                    .forEach(
                        slice ->
                            positionsByBaseFile.put(
                                qualifiedPath(slice.getBaseFile().get()),
                                positionsDeletedIn(slice))));
    return positionsByBaseFile;
  }

  private Set<Long> positionsDeletedIn(FileSlice slice) {
    HoodieFileGroupId fileGroup = slice.getFileGroupId();
    Set<Long> positions = new LinkedHashSet<>();
    slice
        .getLogFiles()
        .collect(Collectors.toList())
        .forEach(
            logFile ->
                collectDeletedPositions(fileGroup, slice.getBaseFile().get(), logFile, positions));
    return positions;
  }

  private Map<HoodieFileGroupId, Set<StoragePath>> logFilesByFileGroup(
      HoodieCommitMetadata commitMetadata, HoodieInstant deltacommit) {
    Map<HoodieFileGroupId, Set<StoragePath>> logFiles = new LinkedHashMap<>();
    commitMetadata
        .getPartitionToWriteStats()
        .forEach(
            (partitionPath, writeStats) -> {
              for (HoodieWriteStat writeStat : writeStats) {
                Set<StoragePath> paths = new LinkedHashSet<>();
                StoragePath path =
                    FSUtils.constructAbsolutePath(metaClient.getBasePath(), writeStat.getPath());
                if (FSUtils.isLogFile(path)) {
                  paths.add(path);
                }
                if (writeStat instanceof HoodieDeltaWriteStat) {
                  // The stat also lists the log files of its handle, which under change data
                  // capture include the ones earlier commits wrote to the file slice.
                  List<String> names = ((HoodieDeltaWriteStat) writeStat).getLogFiles();
                  if (names != null) {
                    StoragePath partition =
                        FSUtils.constructAbsolutePath(metaClient.getBasePath(), partitionPath);
                    names.stream()
                        .map(name -> new StoragePath(partition, name))
                        .filter(
                            logPath ->
                                deltacommit
                                    .requestedTime()
                                    .equals(new HoodieLogFile(logPath).getDeltaCommitTime()))
                        .forEach(paths::add);
                  }
                }
                if (!paths.isEmpty()) {
                  logFiles
                      .computeIfAbsent(
                          new HoodieFileGroupId(partitionPath, writeStat.getFileId()),
                          fileGroup -> new LinkedHashSet<>())
                      .addAll(paths);
                }
              }
            });
    return logFiles;
  }

  private void collectDeletedPositions(
      HoodieFileGroupId fileGroup,
      HoodieBaseFile baseFile,
      HoodieLogFile logFile,
      Set<Long> positions) {
    try (HoodieLogFormat.Reader reader =
        HoodieLogFormat.newReader(metaClient, logFile, tableSchema())) {
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        if (!(block instanceof HoodieDeleteBlock)) {
          throw new NotSupportedException(
              unsupportedLogContent(logFile, "a " + block.getBlockType() + " with records"));
        }
        HoodieDeleteBlock deleteBlock = (HoodieDeleteBlock) block;
        int deletes = deleteBlock.getRecordsToDelete().length;
        List<Long> blockPositions = deleteBlock.getRecordPositionList();
        String baseInstantOfPositions = deleteBlock.getBaseFileInstantTimeOfPositions();
        if (baseInstantOfPositions == null || blockPositions.size() != deletes) {
          throw new NotSupportedException(
              unsupportedLogContent(
                  logFile,
                  String.format(
                      "%d deletes with %d record positions", deletes, blockPositions.size())));
        }
        requirePositionsOf(fileGroup, baseFile, baseInstantOfPositions, logFile);
        positions.addAll(blockPositions);
      }
    } catch (IOException e) {
      throw new ReadException("Unable to read the deletes in log file " + logFile.getPath(), e);
    }
  }

  /**
   * Positions count rows of the base file the writer tagged the deletes against, named by the
   * block's base file instant, and say nothing about any other version of the file group.
   */
  private void requirePositionsOf(
      HoodieFileGroupId fileGroup,
      HoodieBaseFile baseFile,
      String baseInstantOfPositions,
      HoodieLogFile logFile) {
    Option<HoodieBaseFile> positionsBaseFile =
        fsView()
            .getBaseFileOn(
                fileGroup.getPartitionPath(), baseInstantOfPositions, fileGroup.getFileId());
    if (!positionsBaseFile.isPresent()
        || !positionsBaseFile.get().getCommitTime().equals(baseFile.getCommitTime())) {
      throw new NotSupportedException(
          String.format(
              "Log file %s of table %s deletes record positions of the base file written at %s,"
                  + " but the latest base file of file group %s is %s",
              logFile.getPath(),
              metaClient.getTableConfig().getTableName(),
              baseInstantOfPositions,
              fileGroup.getFileId(),
              baseFile.getPath()));
    }
  }

  private String unsupportedLogContent(HoodieLogFile logFile, String found) {
    return String.format(
        "Table %s is published to Iceberg as base files with deletion vectors, so its log files"
            + " may only hold deletes that carry the record position of every deleted row, but"
            + " log file %s holds %s. Write the table with %s=true and an index that supplies"
            + " record positions",
        metaClient.getTableConfig().getTableName(),
        logFile.getPath(),
        found,
        UPDATES_AS_DELETES_AND_INSERTS);
  }

  /** The spelling the base file is recorded under in Iceberg, see {@code HudiDataFileExtractor}. */
  private String qualifiedPath(HoodieBaseFile baseFile) {
    return new StoragePath(baseFile.getPath())
        .makeQualified(metaClient.getStorage().getUri())
        .toString();
  }

  /**
   * The view synced to the timeline as Iceberg has it, on which an instant that Iceberg has not
   * recorded is pending, so the files of the instant being published are not part of the view.
   */
  private SyncableFileSystemView fsView() {
    if (fsView == null) {
      fsView = viewManager.getFileSystemView(metaClient);
      fsView.sync();
    }
    return fsView;
  }

  private HoodieSchema tableSchema() {
    if (tableSchema == null) {
      try {
        tableSchema = new TableSchemaResolver(metaClient).getTableSchema();
      } catch (Exception e) {
        throw new ReadException(
            "Unable to resolve the schema of table " + metaClient.getTableConfig().getTableName(),
            e);
      }
    }
    return tableSchema;
  }
}
