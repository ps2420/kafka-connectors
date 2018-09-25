package com.amaris.kafka.connect.hdp.schema;

import static com.amaris.kafka.connect.spooldir.util.SpooldirUtil.jsonMapper;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amaris.kafka.connect.spooldir.SpoolDirSourceConnector;
import com.amaris.kafka.connect.spooldir.SpoolDirSourceConnectorConfig;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;

public class HDPToKafkaSchemaConvertor {

  private static Logger LOGGER = LoggerFactory.getLogger(SpoolDirSourceConnector.class);

  private static Map<String, String> convertor = new HashMap<String, String>();

  static {
    convertor.put("\"string\"", "\"STRING\"");
    convertor.put("\"long\"", "\"INT64\"");
    convertor.put("\"short\"", "\"INT64\"");
    convertor.put("\"int\"", "\"INT64\"");
    convertor.put("\"int\"", "\"INT64\"");
    convertor.put("optional", "isOptional");
    convertor.put("defaultValue", "default");
  }

  public static Map<String, Object> connectSchemaHeader(final HDPSchema hdpSchema) throws Exception {
    final Map<String, Object> connectSchema = new HashMap<>();
    connectSchema.put("type", "STRUCT");
    connectSchema.put("name", hdpSchema.getNamespace());
    connectSchema.put("isOptional", "false");
    return connectSchema;
  }

  public static String enrichConnectSchema(final Map<String, Object> connectSchema, final HDPSchema hdpSchema) throws Exception {
    final Map<String, Object> payloadMap = converToConnectFieldSchema(hdpSchema);
    connectSchema.put("fieldSchemas", payloadMap);

    String payload = jsonMapper().writeValueAsString(connectSchema);
    payload = payload.replaceAll("optional", "isOptional").replaceAll("defaultValue", "default");

    return payload;
  }

  public static Map<String, Object> converToConnectFieldSchema(final HDPSchema hdpSchema) throws Exception {
    final Map<String, Object> connectFieldMap = new HashMap<>();
    hdpSchema.getFields().stream().forEach(hdpFieldSchema -> {
      try {
        final String json = jsonMapper().writeValueAsString(hdpFieldSchema).toLowerCase();
        final AtomicReference<String> atomic = new AtomicReference<>(json);
        convertor.keySet().stream().forEach(key -> {
          atomic.set(atomic.get().replaceAll(key, convertor.get(key).toUpperCase()));
        });
        final ConnectFieldSchema fieldSchema = jsonMapper().readValue(atomic.get().getBytes(), ConnectFieldSchema.class);
        connectFieldMap.put(hdpFieldSchema.getName(), fieldSchema);
      } catch (final Exception ex) {
      }
    });

    return connectFieldMap;
  }

  private static SchemaRegistryClient getSchemaRegistryClient() {
    Map<String, Object> config = createConfig(SpoolDirSourceConnectorConfig.SCHEMA_REGISTRY_URL);
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

  public static HDPSchema getHortonWorksSchema(final String schemaName) throws Exception {
    String content = new String(Files.readAllBytes(Paths.get("src/test/resources/hdp_schema")));
    content = content.replaceAll("default", "defaultValue");
    final HDPSchema hdpSchema = jsonMapper().readValue(content.getBytes(), HDPSchema.class);
    final String jsonInput = jsonMapper().writeValueAsString(hdpSchema.getFields());
    LOGGER.info("Recevied json-input: [{}]", jsonInput);
    return hdpSchema;
  }

}
