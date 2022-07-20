package com.helios.spark;

import com.helios.spark.sds.client.DatatableSchema;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CSVColumnIndicesInfo {
  private Set<Integer> indicesToAnonymize;
  private Set<Integer> indicesToRemove;

  public static CSVColumnIndicesInfo buildFromSDSDatatableSchemaList(
      List<String> csvColumnNameList, List<DatatableSchema> datatableSchemaList) {
    Map<String, DatatableSchema> existingColumnNameToDatatableSchema =
        datatableSchemaList.stream()
            .collect(Collectors.toMap(DatatableSchema::getColumnName, Function.identity()));
    Set<Integer> columnIndicesToRemove = new HashSet<>();
    Set<Integer> columnIndicesToAnonymize = new HashSet<>();
    for (int i = 0; i < csvColumnNameList.size(); i++) {
      String columnName = csvColumnNameList.get(i);
      if (!existingColumnNameToDatatableSchema.containsKey(columnName)) {
        columnIndicesToRemove.add(i);
        continue;
      }

      if (existingColumnNameToDatatableSchema.get(columnName).isAnonymize()) {
        columnIndicesToAnonymize.add(i);
      }
    }

    return new CSVColumnIndicesInfo(columnIndicesToAnonymize, columnIndicesToRemove);
  }

  public CSVColumnIndicesInfo(Set<Integer> indicesToAnonymize, Set<Integer> indicesToRemove) {
    this.indicesToAnonymize = indicesToAnonymize;
    this.indicesToRemove = indicesToRemove;
  }

  public Set<Integer> getIndicesToAnonymize() {
    return indicesToAnonymize;
  }

  public Set<Integer> getIndicesToRemove() {
    return indicesToRemove;
  }
}
