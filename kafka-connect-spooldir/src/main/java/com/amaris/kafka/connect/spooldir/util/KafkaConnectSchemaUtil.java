package com.amaris.kafka.connect.spooldir.util;

import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

public class KafkaConnectSchemaUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(KafkaConnectSchemaUtil.class);

  private final static MediaType SCHEMA_CONTENT = MediaType.parse("application/vnd.schemaregistry.v1+json");

  public static String listAllSchemas(final String schemaUrl) throws Exception {
    final OkHttpClient client = new OkHttpClient();
    final Request request = new Request.Builder().url(schemaUrl + "/subjects").build(); 
    final String output = client.newCall(request).execute().body().string();
    return output;
  }

  public static String createSchemaFromFile(final String schemaUrl, final String schemaName, final String schemalocation) throws Exception {
    final String schemaContent = new String(Files.readAllBytes(Paths.get(schemalocation)));
    return createSchema(schemaUrl, schemaName, schemaContent);
  }

  public static String createSchema(final String schemaUrl, final String schemaName, final String schemaContent) throws Exception {
    final OkHttpClient client = new OkHttpClient();
    final Request request =
        new Request.Builder().post(RequestBody.create(SCHEMA_CONTENT, schemaContent)).url(buildSchemaUrl(schemaUrl, schemaName)).build();

    final String output = client.newCall(request).execute().body().string();
    LOGGER.info("Schema:[{}] created successfully with id:[{}]", schemaName, output);
    return output;
  }

  public static String fetchSchema(final String schemaUrl, final String schemaName) throws Exception {
    final Request request = new Request.Builder().url(buildSchemaUrl(schemaUrl, schemaName) + "/latest").build();

    final OkHttpClient client = new OkHttpClient();
    final String output = client.newCall(request).execute().body().string();
    return output;
  }

  private static String buildSchemaUrl(final String schemaUrl, final String schemaName) {
    return schemaUrl + "/subjects/" + schemaName + "/versions";
  }
}
