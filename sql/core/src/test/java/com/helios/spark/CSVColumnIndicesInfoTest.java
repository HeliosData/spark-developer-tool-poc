package com.helios.spark;

import com.helios.spark.sds.client.DatatableSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CSVColumnIndicesInfoTest {

  @Test
  public void testBuildFromSDSDatatableSchemaList() {
    List<String> csvColumnNameList =
        new ArrayList<String>() {
          {
            add("column_1");
            add("column_2");
            add("column_3");
          }
        };
    List<DatatableSchema> datatableSchemaList =
        new ArrayList<DatatableSchema>() {
          {
            add(new DatatableSchema("column_1", "string", true));
            add(new DatatableSchema("column_3", "string", false));
          }
        };
    Set<Integer> indicesToAnonymize =
        new HashSet<Integer>() {
          {
            add(0);
          }
        };
    Set<Integer> indicesToRemove =
        new HashSet<Integer>() {
          {
            add(1);
          }
        };
    CSVColumnIndicesInfo expected = new CSVColumnIndicesInfo(indicesToAnonymize, indicesToRemove);

    CSVColumnIndicesInfo actual =
        CSVColumnIndicesInfo.buildFromSDSDatatableSchemaList(
            csvColumnNameList, datatableSchemaList);

    Assert.assertEquals(expected.getIndicesToAnonymize(), actual.getIndicesToAnonymize());
    Assert.assertEquals(expected.getIndicesToRemove(), actual.getIndicesToRemove());
  }
}
