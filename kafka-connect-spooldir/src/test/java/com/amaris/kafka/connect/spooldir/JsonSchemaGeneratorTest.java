package com.amaris.kafka.connect.spooldir;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import com.amaris.kafka.connect.spooldir.JsonSchemaGenerator;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
 
public class JsonSchemaGeneratorTest extends SchemaGeneratorTest {

  @Test
  public void schema() throws IOException {
    File inputFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/json/FieldsMatch.data");
    JsonSchemaGenerator schemaGenerator = new JsonSchemaGenerator(settings);
    Map.Entry<Schema, Schema> kvp = schemaGenerator.generate(inputFile, Arrays.asList("id"));
    final Schema expectedKeySchema = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.model.Key")
        .field("id", Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    final Schema expectedValueSchema = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.model.Value")
        .field("id", Schema.OPTIONAL_STRING_SCHEMA)
        .field("first_name", Schema.OPTIONAL_STRING_SCHEMA)
        .field("last_name", Schema.OPTIONAL_STRING_SCHEMA)
        .field("email", Schema.OPTIONAL_STRING_SCHEMA)
        .field("gender", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ip_address", Schema.OPTIONAL_STRING_SCHEMA)
        .field("last_login", Schema.OPTIONAL_STRING_SCHEMA)
        .field("account_balance", Schema.OPTIONAL_STRING_SCHEMA)
        .field("country", Schema.OPTIONAL_STRING_SCHEMA)
        .field("favorite_color", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
 
  }


}
