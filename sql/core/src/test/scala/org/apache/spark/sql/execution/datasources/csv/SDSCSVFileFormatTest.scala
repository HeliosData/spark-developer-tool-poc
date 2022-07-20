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

import com.helios.spark.sds.client.{DatatableSchema, SDSClient}
import org.apache.hadoop.conf.Configuration
import org.mockito.Mockito.when
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.mockito.MockitoSugar.mock

class SDSCSVFileFormatTest extends FunSpec with Matchers {
  val fileFormat = new SDSCSVFileFormat()

  describe("Test setSDSRequiredConfigIntoHadoopConfig") {
    it("should set required config") {
      val sep = "|"
      val comment = "#"
      val options: Map[String, String] = Map("sep" -> sep)
      val parsedOptions = new CSVOptions(options, false, "", "")
      val hadoopConf = new Configuration()

      val mockSDSClient = mock[SDSClient]
      val sdsDatatableSchemaList = JavaConverters.seqAsJavaListConverter(Vector(
        new DatatableSchema("column_1", "string", true),
        new DatatableSchema("column_2", "integer", false)
      )).asJava
      when(mockSDSClient.getDatatableSchema("", "")) thenReturn (sdsDatatableSchemaList)
      fileFormat.setSDSClient(mockSDSClient)

      fileFormat.setSDSRequiredConfigIntoHadoopConfig(options, parsedOptions, hadoopConf)

      assert(hadoopConf.get("sdsDatatableSchema") !== null)
      assert(hadoopConf.get("csvFieldSep") == sep)
      assert(hadoopConf.getBoolean("csvFileCommentSet", false) == false)

      val optionsWithComment = options + (("comment", comment))
      val parsedOptionsWithComment = new CSVOptions(optionsWithComment, false, "", "")
      fileFormat.setSDSRequiredConfigIntoHadoopConfig(optionsWithComment, parsedOptionsWithComment,
        hadoopConf)

      assert(hadoopConf.getBoolean("csvFileCommentSet", false))
      assert(hadoopConf.get("csvFileComment") == comment)
    }
  }
}
