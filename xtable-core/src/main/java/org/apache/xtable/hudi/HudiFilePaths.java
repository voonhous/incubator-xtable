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
 
package org.apache.xtable.hudi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.xtable.exception.ReadException;

/**
 * Hudi hands out the same file under two spellings: paths built from commit metadata keep the
 * table's base path as the writer gave it (for example {@code /warehouse/t/f.parquet}), while paths
 * from file listings are fully qualified ({@code file:/warehouse/t/f.parquet} or {@code
 * hdfs://nn/warehouse/t/f.parquet}). Target formats such as Iceberg identify data files by their
 * path string, so every path XTable reports must use one spelling; this class produces the fully
 * qualified one.
 */
public final class HudiFilePaths {
  private HudiFilePaths() {}

  /**
   * @return the path with the scheme and authority of its file system filled in; already qualified
   *     paths are returned unchanged
   */
  public static String qualify(String path, Configuration conf) {
    Path hadoopPath = new Path(path);
    try {
      return hadoopPath.getFileSystem(conf).makeQualified(hadoopPath).toString();
    } catch (IOException e) {
      throw new ReadException("Unable to resolve the file system of " + path, e);
    }
  }
}
