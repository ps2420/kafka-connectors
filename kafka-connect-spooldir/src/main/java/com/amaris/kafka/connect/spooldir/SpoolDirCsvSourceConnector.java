
package com.amaris.kafka.connect.spooldir;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.Title;

@Title("CSV Source Connector")
@Description("The SpoolDirCsvSourceConnector will monitor the directory specified in `input.path` for files and read them as a CSV "
    + "converting each of the records to the strongly typed equivalent specified in `key.schema` and `value.schema`.")

public class SpoolDirCsvSourceConnector extends SpoolDirSourceConnector<SpoolDirCsvSourceConnectorConfig> {

  @Override
  protected SpoolDirCsvSourceConnectorConfig config(Map<String, String> settings) {

    final String file_pattern = settings.get(SpoolDirCsvSourceConnectorConfig.INPUT_FILE_PATTERN_CONF);
    settings.put(SpoolDirCsvSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.*\\" + "." + file_pattern + "$");
    return new SpoolDirCsvSourceConnectorConfig(false, settings);
  }

  @Override
  protected SchemaGenerator<SpoolDirCsvSourceConnectorConfig> generator(Map<String, String> settings) {
    return new CsvSchemaGenerator(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirCsvSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirCsvSourceConnectorConfig.conf();
  }
}
