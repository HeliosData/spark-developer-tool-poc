package com.helios.spark.sds.client;

public interface SDSClient {
  void validateDevelopLicense(String license);

  DatatableSchemaResponse getDatatableSchema(String license, String tablePath);
}
