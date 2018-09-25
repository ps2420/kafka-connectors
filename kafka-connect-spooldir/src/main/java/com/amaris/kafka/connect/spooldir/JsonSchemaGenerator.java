package com.amaris.kafka.connect.spooldir;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import org.apache.kafka.connect.data.Schema;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonSchemaGenerator extends SchemaGenerator<SpoolDirJsonSourceConnectorConfig> {
  public JsonSchemaGenerator(Map<String, ?> settings) {
    super(settings);
  }

  @Override
  protected SpoolDirJsonSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirJsonSourceConnectorConfig(false, settings);
  }

  @Override
  protected Map<String, Schema.Type> determineFieldTypes(InputStream inputStream) throws IOException {
    Map<String, Schema.Type> typeMap = new LinkedHashMap<>();
    JsonFactory factory = new JsonFactory();
    try (JsonParser parser = factory.createParser(inputStream)) {
      Iterator<JsonNode> iterator = ObjectMapperFactory.INSTANCE.readValues(parser, JsonNode.class);
      while (iterator.hasNext()) {
        JsonNode node = iterator.next();
        if (node.isObject()) {
          Iterator<String> fieldNames = node.fieldNames();
          while (fieldNames.hasNext()) {
            typeMap.put(fieldNames.next(), Schema.Type.STRING);
          }
          break;
        }
      }
    }
    return typeMap;
  }
}
