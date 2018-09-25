package com.amaris.kafka.connect.spooldir.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class SpooldirUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpooldirUtil.class);

  public final static String SPECIAL_CHARS = "[\\!\\`\\'\\,\\.\\+\\_\\-\\\"]";

  private static ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
    objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
  }

  public static ObjectMapper jsonMapper() {
    return objectMapper;
  }

  public static List<String> filterSpecialCharList(final String[] _fieldNames) {
    final List<String> fieldWithoutSpecialChars = Arrays.asList(_fieldNames).stream().map(field -> {
      try {
        field = StringUtils.trimLeadingWhitespace(field.replaceAll(SPECIAL_CHARS, " ")).trim();
        field = field.replaceAll("\\s+", "_").toLowerCase();
        field = StringUtils.trimAllWhitespace(field).toLowerCase();
      } catch (final Exception ex) {
        LOGGER.warn("Unable to remove special charactrers from csv file header : [{}] " + ex, field);
      }
      return StringUtils.trimAllWhitespace(field).toLowerCase();
    }).collect(Collectors.toList());
    LOGGER.info("Fieldnames after removing special charactrers :[{}]", fieldWithoutSpecialChars);
    return fieldWithoutSpecialChars;
  }

  public static String readFromInputStream(final InputStream inputStream) throws IOException {
    StringBuilder resultStringBuilder = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = br.readLine()) != null) {
        resultStringBuilder.append(line).append("\n");
      }
    }
    return resultStringBuilder.toString();
  }

  public static String loadKafkaKeySchema(final String schema_name) throws IOException {
    final InputStream is = SpooldirUtil.class.getClassLoader().getResourceAsStream(schema_name);
    final String schema = SpooldirUtil.readFromInputStream(is);
    return schema;
  }

  public static Tuple2<Struct, Struct> getFileAuditEvent(final Schema keySchema, final Schema valueSchema, final String fileName, final String event) throws Exception {
    final Struct keyStruct = new Struct(keySchema);
    final Field keyField = keySchema.field("uuid");
    keyStruct.put(keyField, java.util.UUID.randomUUID().toString());
    
    final Field uuid = valueSchema.field("uuid");
    final Field file_name = valueSchema.field("filename");
    final Field event_field = valueSchema.field("event");
    final Field audit_time = valueSchema.field("audit_time");

    final Struct valueStruct = new Struct(valueSchema);
    valueStruct.put(uuid, java.util.UUID.randomUUID().toString());
    valueStruct.put(file_name, fileName);
    valueStruct.put(event_field, event);
    valueStruct.put(audit_time, System.currentTimeMillis());

    return Tuples.of(keyStruct, valueStruct);
  }


}


