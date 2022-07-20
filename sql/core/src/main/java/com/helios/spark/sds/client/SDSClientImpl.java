package com.helios.spark.sds.client;

import java.util.ArrayList;
import java.util.List;

public class SDSClientImpl implements SDSClient {
  @Override
  public void validateDevelopLicense(String license) {}

  @Override
  public List<DatatableSchema> getDatatableSchema(String license, String tablePath) {
    // TODO: [SDS] get from sds client
    return new ArrayList<DatatableSchema>() {
      {
        add(new DatatableSchema("year", "string", true));
        add(new DatatableSchema("model", "string", false));
        add(new DatatableSchema("comment", "string", false));
        add(new DatatableSchema("blank", "string", false));
      }
    };
  }
}
