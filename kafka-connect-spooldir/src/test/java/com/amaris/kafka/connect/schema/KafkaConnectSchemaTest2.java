package com.amaris.kafka.connect.schema;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amaris.kafka.connect.spooldir.util.KafkaConnectSchemaUtil;

public class KafkaConnectSchemaTest2 {

  private static Logger LOGGER = LoggerFactory.getLogger(KafkaConnectSchemaTest2.class);

  public static String SCHEMA_URL = "http://localhost:8081";
  public static String SCHEMA_NAME = "test";

  private final static String EMPLOYEE_SCHEMA = "{\n" + "  \"schema\": \"" + "  {" + "    \\\"namespace\\\": \\\"com.cloudurable.phonebook\\\","
      + "    \\\"type\\\": \\\"record\\\"," + "    \\\"name\\\": \\\"Employee\\\"," + "    \\\"fields\\\": ["
      + "        {\\\"name\\\": \\\"fName\\\", \\\"type\\\": \\\"string\\\"},"
      + "        {\\\"name\\\": \\\"lName\\\", \\\"type\\\": \\\"string\\\"}," + "        {\\\"name\\\": \\\"age\\\",  \\\"type\\\": \\\"int\\\"},"
      + "        {\\\"name\\\": \\\"phoneNumber\\\",  \\\"type\\\": \\\"string\\\"}" + "    ]" + "  }\"" + "}";

  public static void main(String[] args) throws Exception { 
    createSchema();
    // createSchema();
    // listAllSchemas();
    //printSchemaText();
  }

  public static void createSchema() throws Exception {
    final String currentLocation = new File(".").getCanonicalPath();
    final String path = currentLocation + "/src/test/resources/employee.avsc";
    final String schemaId = KafkaConnectSchemaUtil.createSchemaFromFile(SCHEMA_URL, java.util.UUID.randomUUID().toString(), path);
    LOGGER.info("Schema-id: [{}]", schemaId);
  }

  public static void listAllSchemas() throws Exception {
    final String schemas = KafkaConnectSchemaUtil.listAllSchemas(SCHEMA_URL);
    LOGGER.info("Listing all schemas :[{}] ", schemas);
  }

  public static void printSchemaText() throws Exception {
    final String schemaContent = KafkaConnectSchemaUtil.fetchSchema(SCHEMA_URL, SCHEMA_NAME);
    LOGGER.info("Listing all schemas :[{}] ", schemaContent);
  }

}
