package com.helios.spark.sds.client;

public interface SDSClient {
    void validateDevelopLicense(String license);
    void getDatatableSchema(String license, String tablePath);
}
