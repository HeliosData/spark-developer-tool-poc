package com.helios.spark.sds.client;

import java.util.List;

public interface SDSClient {
  void validateDevelopLicense(String license);

  List<DatatableSchema> getDatatableSchema(String license, String tablePath);
}
