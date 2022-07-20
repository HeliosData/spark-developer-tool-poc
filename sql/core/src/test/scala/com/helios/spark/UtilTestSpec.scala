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

import com.helios.spark.sds.client.DatatableSchema
import org.apache.hadoop.conf.Configuration
import org.scalatest.{FunSpec, Matchers}

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class UtilTestSpec extends FunSpec with Matchers {
  describe("Test getSDSDatatableSchema") {
    it("should return expected list") {
      val conf = new Configuration()
      val expected = Vector(
        new DatatableSchema("column_1", "string", true),
        new DatatableSchema("column_2", "integer", false)
      )
      Util.setSDSDatatableSchema(conf, expected)

      val actual = Util.getSDSDatatableSchema(conf)

      assert(expected.size == actual.size)
      expected.zip(actual).foreach((datatableSchemas) => {
        assert(sameDatatableSchema(datatableSchemas._1, datatableSchemas._2))
      })
    }
  }

  describe("Test getNewDataSchemaFromDatatableSchema") {
    it("should ignore fields which are not exist in datatable schema sequence") {
      val originalDataSchema = new StructType(Array[StructField](
        DataTypes.createStructField("column_1", DataTypes.StringType, false),
        DataTypes.createStructField("column_2", DataTypes.StringType, false),
        DataTypes.createStructField("column_3_should_be_ignore", DataTypes.StringType, false)
      ))
      val datatableSchemaSeq = Vector(
        new DatatableSchema("column_1", "string", false),
        new DatatableSchema("column_2", "integer", false)
      )
      val expected = new StructType(Array[StructField](
        DataTypes.createStructField("column_1", DataTypes.StringType, false),
        DataTypes.createStructField("column_2", DataTypes.StringType, false)
      ))

      val actual = Util.getNewDataSchemaFromDatatableSchema(originalDataSchema, datatableSchemaSeq)
      assert(expected == actual)
    }
  }

  def sameDatatableSchema(a: DatatableSchema, b: DatatableSchema): Boolean = {
    a.getColumnName == b.getColumnName &&
      a.getType == b.getType &&
      a.isAnonymize == b.isAnonymize
  }
}
