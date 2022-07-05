package com.helios.spark.sds.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class DatatableSchema {
  private String columnName;
  private String type;
  private boolean isAnonymize;

  public DatatableSchema() {
  }

  public DatatableSchema(String columnName, String type, boolean isAnonymize) {
    this.columnName = columnName;
    this.type = type;
    this.isAnonymize = isAnonymize;
  }

  public static DatatableSchema fromJsonString(String str) {
    try {
      return new ObjectMapper().readValue(str, DatatableSchema.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setAnonymize(boolean anonymize) {
    isAnonymize = anonymize;
  }

  public String toString() {
    ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
    try {
      return objectWriter.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
