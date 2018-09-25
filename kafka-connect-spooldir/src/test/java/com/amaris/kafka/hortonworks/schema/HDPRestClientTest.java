package com.amaris.kafka.hortonworks.schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;

public class HDPRestClientTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(HDPRestClientTest.class);

  static SchemaRegistryClient schemaRegistryClient = null;

  public static final String DEFAULT_SCHEMA_REG_URL = "http://192.168.1.35:9001/api/v1";

  public static void main(String[] args) throws Exception {
    schemaRegistryClient = getSchemaRegistryClient();
    //createDefaultSchema();
    readSchema();
  }

  final static String SCHEMA_DEVICE = "device";
  final static String SCHEMA_DEVICE_1 = "Device9";

  public static void readSchema() throws Exception {
    final SchemaVersionInfo versionInfo = schemaRegistryClient.getLatestSchemaVersionInfo("apple_poc");
    System.out.println(versionInfo.getVersion() + " =  " + versionInfo.getSchemaText());
  }

  public static void createDefaultSchema() throws Exception {
    final String schemaFileName = "/avro/schema/device-next.avsc";
    final String schema1 = getSchema(schemaFileName);

    createSchemaMetaData();
    updateSchema();

    // final SchemaIdVersion schemaVersionId =
    // schemaRegistryClient.addSchemaVersion(SCHEMA_DEVICE_1, new SchemaVersion(schema1, "Initial
    // version of the schema"));

    // LOGGER.info("VERSION :[{}]", schemaVersionId.getVersion());
  }

  private static void updateSchema() throws Exception {
    final Client client = ClientBuilder.newClient();
    final String url = "http://localhost:9090/api/v1/schemaregistry/schemas/" + SCHEMA_DEVICE_1 + "/versions";
    final WebTarget webTarget = client.target(url);
    final Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

    final String schemaFileName = "/avro/schema/device-next.avsc";
    final String schema1 = getSchema(schemaFileName);

    String metadata = "{\"schemaText\":\"" + schema1 + "\",\"description\":\"" + SCHEMA_DEVICE_1 + "\"}";
    final SchemaVersion schemaVersion = new SchemaVersion(schema1, SCHEMA_DEVICE_1);

    metadata = getSchema("/avro/schema/schema_update.asvc");

    LOGGER.info("updating schema : {} \n url:{}", metadata, url);
    Response response = invocationBuilder.post(Entity.entity(metadata, MediaType.APPLICATION_JSON));
    LOGGER.info(response.getStatus() + " description : " + response.getStatusInfo().toString());
  }


  private static void createSchemaMetaData() {

    final Client client = ClientBuilder.newClient();
    final WebTarget webTarget = client.target("http://localhost:9090/api/v1/schemaregistry/schemas");
    final Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);

    final String metadata = "{\"name\":\"" + SCHEMA_DEVICE_1 + "\",\"type\":\"avro\",\"schemaGroup\":\"avro\",\"description\":\"" + SCHEMA_DEVICE_1
        + "\",\"evolve\":true,\"compatibility\":\"BACKWARD\"}";

    Response response = invocationBuilder.post(Entity.entity(metadata, MediaType.APPLICATION_JSON));
    LOGGER.info(response.getStatus() + " description : " + response.getStatusInfo().toString());


    final SchemaMetadataInfo schema_data_info = schemaRegistryClient.getSchemaMetadataInfo(SCHEMA_DEVICE_1);
    LOGGER.info("schema_data_info : " + schema_data_info);
  }

  private static SchemaMetadata createSchemaMetadata(String name) {
    return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup("sample-group").description("Sample schema")
        .compatibility(SchemaCompatibility.BACKWARD).validationLevel(null).build();
  }

  private static String getSchema(String schemaFileName) throws IOException {
    final InputStream schemaResourceStream = HDPRestClientTest.class.getResourceAsStream(schemaFileName);
    return IOUtils.toString(schemaResourceStream, "UTF-8");
  }

  private static SchemaRegistryClient getSchemaRegistryClient() {
    Map<String, Object> config = createConfig(DEFAULT_SCHEMA_REG_URL);
    return new SchemaRegistryClient(config);
  }

  public static Map<String, Object> createConfig(String schemaRegistryUrl) {
    Map<String, Object> config = new HashMap<>();
    config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
    config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
    config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000L);
    config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000L);
    config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000L);
    return config;
  }

}
