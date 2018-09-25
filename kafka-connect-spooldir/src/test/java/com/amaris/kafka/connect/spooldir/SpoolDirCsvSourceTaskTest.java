
package com.amaris.kafka.connect.spooldir;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import com.google.common.io.Files;

public class SpoolDirCsvSourceTaskTest extends SpoolDirSourceTaskTest<SpoolDirCsvSourceTask> {

  @Override
  protected SpoolDirCsvSourceTask createTask() {
    return new SpoolDirCsvSourceTask();
  }

  @Override
  protected void settings(Map<String, String> settings) {
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
    settings.put(SpoolDirCsvSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF, "BOTH");
    settings.put("halt.on.error", "false");
    settings.put("schema.generation.enabled", "true");
    settings.put("topic", "test1");
    settings.put("csv.first.row.as.header", "true");
  }

  @TestFactory
  public Stream<DynamicTest> poll() throws IOException {
    final String packageName = "csv";
    List<TestCase> testCases = loadTestCases(packageName);

    return testCases.stream().map(testCase -> {
      String name = Files.getNameWithoutExtension(testCase.path.toString());
      return dynamicTest(name, () -> {
        poll(packageName, testCase);
      });
    });
  }
}
