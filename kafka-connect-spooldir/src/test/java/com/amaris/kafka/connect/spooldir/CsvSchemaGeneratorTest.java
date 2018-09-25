/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amaris.kafka.connect.spooldir;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import com.amaris.kafka.connect.spooldir.CsvSchemaGenerator;
import com.amaris.kafka.connect.spooldir.SpoolDirCsvSourceConnectorConfig;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
 
public class CsvSchemaGeneratorTest extends SchemaGeneratorTest {

  @Test
  public void foo() throws IOException {
    File inputFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/csv/FieldsMatch.data");
    this.settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
    CsvSchemaGenerator schemaGenerator = new CsvSchemaGenerator(settings);
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
