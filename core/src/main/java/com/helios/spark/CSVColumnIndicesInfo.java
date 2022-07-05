package com.helios.spark;

import com.helios.spark.sds.client.DatatableSchema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CSVColumnIndicesInfo {
  private Set<Integer> indicesToAnonymize;
  private Set<Integer> indicesToRemove;

  public static CSVColumnIndicesInfo buildFromSDSDatatableSchemaList(
      List<DatatableSchema> datatableSchemaList) {
    // TODO: fix
    Set<Integer> a = new HashSet<>();
    a.add(0);
    Set<Integer> b = new HashSet<>();
    b.add(1);
    return new CSVColumnIndicesInfo(a, b);
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
