package com.helios.spark.sds.client;

public class DatatableSchema {
  private final String columnName;
  private final String type;
  private final boolean isAnonymize;

  public DatatableSchema(String columnName, String type, boolean isAnonymize) {
    this.columnName = columnName;
    this.type = type;
    this.isAnonymize = isAnonymize;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getType() {
    return type;
  }

  public boolean isAnonymize() {
    return isAnonymize;
  }
}
