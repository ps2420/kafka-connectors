package com.amaris.kafka.connect.spooldir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amaris.kafka.connect.spooldir.util.SpooldirUtil;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;

public abstract class SpoolDirSourceConnector<CONF extends SpoolDirSourceConnectorConfig> extends SourceConnector {

  private static Logger LOGGER = LoggerFactory.getLogger(SpoolDirSourceConnector.class);

  protected Map<String, String> settings;

  private CONF config;

  protected abstract CONF config(Map<String, String> settings);

  protected abstract SchemaGenerator<CONF> generator(Map<String, String> settings);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(final Map<String, String> input) {
    try {
      this.config = config(input);
      final Map<String, String> settings = new LinkedHashMap<>(input);

      final String schema_dir = input.get(SpoolDirSourceConnectorConfig.SCHEMA_DIR);
      final String schema_name = input.get(SpoolDirSourceConnectorConfig.SCHEMA_NAME);
      
      final String keySchema = SpooldirUtil.loadKafkaKeySchema("kafka_key.schema");
      final String errorPayloadSchema = SpooldirUtil.loadKafkaKeySchema(SpoolDirSourceConnectorConfig.ERROR_PAYLOAD_SCHEMA);
       
      String valueSchema = "";
      if (new File(schema_dir + "/" + schema_name + "_value.schema").exists()) {
        valueSchema = new String(Files.readAllBytes(Paths.get(schema_dir + "/" + schema_name + "_value.schema")));
      }
       
      final String fileAuditKeySchema = SpooldirUtil.loadKafkaKeySchema("file_audit_key.schema");
      final String fileAuditValueSchema = SpooldirUtil.loadKafkaKeySchema("file_audit_value.schema");

      settings.put(SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, valueSchema);
      settings.put(SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, keySchema);
      
      settings.put(SpoolDirSourceConnectorConfig.ERROR_PAYLOAD_SCHEMA, errorPayloadSchema);

      settings.put(SpoolDirSourceConnectorConfig.FILE_AUDIT_KEY_SCHEMA_CONF, fileAuditKeySchema);
      settings.put(SpoolDirSourceConnectorConfig.FILE_AUDIT_VALUE_SCHEMA_CONF, fileAuditValueSchema);

      this.settings = settings;
    } catch (final Exception ex) {
      LOGGER.error("Error in loading value schema " + ex, ex);
    }
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return Arrays.asList(this.settings);
  }

  @Override
  public void stop() {

  }
}
