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

package com.helios.spark

import java.lang.reflect.Type

import scala.collection.JavaConverters
import scala.util.control.Breaks._

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.helios.spark.sds.client.DatatableSchema
import java.util
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.datasources.csv.CSVUtils
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.StructType

class Util {
}

object Util {
  val gson = new Gson
  val CSV_HEADER_STR = "csvHeaderStr"
  val CSV_FIELD_SEP = "csvFieldSep"
  val CSV_FILE_COMMENT = "csvFileComment"
  val CSV_FILE_COMMENT_SET = "csvFileCommentSet"
  val SDS_DATATABLE_SCHEMA = "sdsDatatableSchema"

  val OPTION_KEY_SDS_DEVELOPER_LICENSE = "sdsDeveloperLicense"
  val OPTION_KEY_SDS_DATATABLE_PATH = DataSourceOptions.PATH_KEY

  def getCSVHeaderStr(conf: Configuration): String = {
    conf.get(CSV_HEADER_STR)
  }

  def setCSVHeaderStr(conf: Configuration, dataSchema: StructType): Unit = {
    conf.set(CSV_HEADER_STR, dataSchema.fieldNames.mkString(","))
  }

  def setCSVHeaderStr(conf: Configuration, csvHeaderStr: String): Unit = {
    conf.set(CSV_HEADER_STR, csvHeaderStr)
  }

  def getCSVFieldSep(conf: Configuration): Char = {
    CSVUtils.toChar(conf.get(CSV_FIELD_SEP))
  }

  def setCSVFieldSep(conf: Configuration, sep: Char): Unit = {
    conf.set(CSV_FIELD_SEP, sep.toString)
  }

  def getCSVFileComment(conf: Configuration): Char = {
    CSVUtils.toChar(conf.get(CSV_FILE_COMMENT))
  }

  def setCSVFileComment(conf: Configuration, sep: Char): Unit = {
    conf.set(CSV_FILE_COMMENT, sep.toString)
  }

  def getIsCSVFileCommentSet(conf: Configuration): Boolean = {
    conf.getBoolean(CSV_FILE_COMMENT_SET, false)
  }

  def setIsCSVFileCommentSet(conf: Configuration, isSet: Boolean): Unit = {
    conf.setBoolean(CSV_FILE_COMMENT_SET, isSet)
  }

  def getSDSDatatableSchema(conf: Configuration): Seq[DatatableSchema] = {
    val s = conf.get(SDS_DATATABLE_SCHEMA)
    if (s == null) {
      return Vector.empty
    }
    val listType: Type = new TypeToken[util.ArrayList[DatatableSchema]]() {}.getType()
    printf("getSDSDatatableSchema: %s \n", s)
    val arrayList: util.ArrayList[DatatableSchema] = (gson.fromJson(s, listType))
    JavaConverters.asScalaIteratorConverter(arrayList.iterator()).asScala.toVector
  }

  def setSDSDatatableSchema(conf: Configuration, list: Seq[DatatableSchema]): Unit = {
    val s = gson.toJson(scala.collection.JavaConverters.seqAsJavaListConverter(list).asJava)
    printf("setSDSDatatableSchema: %s \n", s)
    conf.set(SDS_DATATABLE_SCHEMA, s)
  }

  def getNewDataSchemaFromDatatableSchema(
    originDataSchema: StructType,
    datatableSchemaList: Seq[DatatableSchema]): StructType = {
    val columnNameToDatatableSchema =
      datatableSchemaList.map(datatableSchema => datatableSchema.getColumnName ->
        datatableSchema).toMap
    var newDataSchema = new StructType()
    originDataSchema.fields.foreach {
      field =>
        breakable {
          if (!columnNameToDatatableSchema.contains(field.name)) {
            break()
          }
          newDataSchema = newDataSchema.add(field)
        }
    }
    newDataSchema
  }
}
