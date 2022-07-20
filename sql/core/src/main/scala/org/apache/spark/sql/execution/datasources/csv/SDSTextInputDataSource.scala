/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.csv

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

object SDSTextInputDataSourc extends CSVDataSource {
  override def isSplitable: Boolean = TextInputCSVDataSource.isSplitable

  override def readFile(
    conf: Configuration,
    file: PartitionedFile,
    parser: UnivocityParser,
    requiredSchema: StructType,
    dataSchema: StructType,
    caseSensitive: Boolean,
    columnPruning: Boolean): Iterator[InternalRow] = {
    val hasHeader = parser.options.headerFlag && file.start == 0
    val lines = {
      val linesReader = new SDSHadoopFileLinesReader(file, conf, hasHeader)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
      linesReader.map { line =>
        new String(line.getBytes, 0, line.getLength, parser.options.charset)
      }
    }

    UnivocityParser.parseIterator(lines, parser, requiredSchema)
  }

  override def infer(
    sparkSession: SparkSession,
    inputPaths: Seq[FileStatus],
    parsedOptions: CSVOptions): StructType = {
    TextInputCSVDataSource.infer(sparkSession, inputPaths, parsedOptions)
  }

  /**
   * Infers the schema from `Dataset` that stores CSV string records.
   */
  def inferFromDataset(
    sparkSession: SparkSession,
    csv: Dataset[String],
    maybeFirstLine: Option[String],
    parsedOptions: CSVOptions): StructType = {
    TextInputCSVDataSource.inferFromDataset(sparkSession, csv, maybeFirstLine, parsedOptions)
  }
}
