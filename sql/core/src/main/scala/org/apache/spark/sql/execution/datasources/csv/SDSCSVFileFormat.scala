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

import scala.collection.JavaConverters

import com.helios.spark.Util
import com.helios.spark.sds.client.{DatatableSchema, SDSClient, SDSClientImpl}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Provides access to CSV data from pure SQL statements.
 */
class SDSCSVFileFormat extends CSVFileFormat {
  var sdsClient: SDSClient = new SDSClientImpl()

  def setSDSClient(sdsClient: SDSClient): Unit = {
    this.sdsClient = sdsClient
  }

  def getSDSDatatableSchema(license: String, tablePath: String): Seq[DatatableSchema] = {
    val datatableSchemas = this.sdsClient.getDatatableSchema(license, tablePath)
    JavaConverters.asScalaIteratorConverter(datatableSchemas.iterator()).asScala.toSeq
  }

  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = new CSVOptions(
      options,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)

    val sdsDeveloperLicense = options.get(Util.OPTION_KEY_SDS_DEVELOPER_LICENSE)
    val sdsDatatablePath = options.get(Util.OPTION_KEY_SDS_DATATABLE_PATH)

    // TODO: [SDS] Move SparkSession validation to session creation
//    if (sdsDeveloperLicense.isEmpty || sdsDatatablePath.isEmpty) {
//      throw new RuntimeException("must provide \"%s\" and \"%s\" in option"
//        .format(Util.OPTION_KEY_SDS_DEVELOPER_LICENSE, Util.OPTION_KEY_SDS_DATATABLE_PATH))
//    }
    updateSchemaBySDSSchemaInfo(
      CSVDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions),
      sdsDeveloperLicense.getOrElse(""), sdsDatatablePath.getOrElse(""))

    // Note: [TestForSDSCSVSuite] Use original logic to return schema
    // CSVDataSource(parsedOptions).inferSchema(sparkSession, files, parsedOptions)
  }

  def updateSchemaBySDSSchemaInfo(
    schema: Option[StructType],
    license: String,
    tablePath: String
  ): Option[StructType] = {
    if (schema.isEmpty) {
      return schema
    }

    val sdsDatatableSchemaList = this.getSDSDatatableSchema(license, tablePath)
    Some(Util.getNewDataSchemaFromDatatableSchema(schema.get, sdsDatatableSchemaList))
  }

  override def buildReader(
    sparkSession: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val parsedOptions = new CSVOptions(
      options,
      sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    setSDSRequiredConfigIntoHadoopConfig(options, parsedOptions, hadoopConf)

    super.buildReader(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      filters,
      options,
      hadoopConf)
  }

  def setSDSRequiredConfigIntoHadoopConfig(
    options: Map[String, String],
    parsedOptions: CSVOptions,
    hadoopConf: Configuration): Unit = {
    val sdsDatatableSchemaList = this.getSDSDatatableSchema(
      options.getOrElse(Util.OPTION_KEY_SDS_DEVELOPER_LICENSE, ""),
      options.getOrElse(Util.OPTION_KEY_SDS_DATATABLE_PATH, ""))

    Util.setSDSDatatableSchema(hadoopConf, sdsDatatableSchemaList)
    Util.setCSVFieldSep(hadoopConf, CSVUtils.toChar(
      options.getOrElse("sep", options.getOrElse("delimiter", ","))))

    if (parsedOptions.isCommentSet) {
      Util.setCSVFileComment(hadoopConf, parsedOptions.comment)
      Util.setIsCSVFileCommentSet(hadoopConf, isSet = true)
    }
  }
}
