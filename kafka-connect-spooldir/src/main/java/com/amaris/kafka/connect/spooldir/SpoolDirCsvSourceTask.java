
package com.amaris.kafka.connect.spooldir;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amaris.kafka.connect.spooldir.util.SpooldirUtil;
import com.google.common.base.Joiner;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class SpoolDirCsvSourceTask extends SpoolDirSourceTask<SpoolDirCsvSourceConnectorConfig> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpoolDirCsvSourceTask.class);

  String[] fieldNames;
  private CSVParser csvParser;
  private CSVReader csvReader;
  private InputStreamReader streamReader;
  private Map<String, String> fileMetadata;

  @Override
  protected SpoolDirCsvSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirCsvSourceConnectorConfig(true, settings);
  }

  @Override
  protected void configure(InputStream inputStream, Map<String, String> metadata, final Long lastOffset) throws IOException {
    LOGGER.debug("====> SpoolDirCsvSourceTask configure() method being initialized...");

    this.csvParser = this.config.createCSVParserBuilder().build();
    this.streamReader = new InputStreamReader(inputStream, this.config.charset);
    CSVReaderBuilder csvReaderBuilder = this.config.createCSVReaderBuilder(this.streamReader, csvParser);
    this.csvReader = csvReaderBuilder.build();

    String[] fieldNames;
    if (this.config.firstRowAsHeader) {
      fieldNames = this.csvReader.readNext();
      final List<String> specialCharList = com.amaris.kafka.connect.spooldir.util.SpooldirUtil.filterSpecialCharList(fieldNames);
      fieldNames = specialCharList.toArray(new String[specialCharList.size()]);
      LOGGER.info("==> reading file headers :[{}]", Arrays.asList(fieldNames));
    } else {
      fieldNames = new String[this.config.valueSchema.fields().size()];
      int index = 0;
      for (Field field : this.config.valueSchema.fields()) {
        fieldNames[index++] = field.name();
      }
      LOGGER.info("configure() - field names from schema order. fields = {}", Joiner.on(", ").join(fieldNames));
    }

    if (null != lastOffset) {
      LOGGER.info("Found previous offset. Skipping {} line(s).", lastOffset.intValue());
      String[] row = null;
      while (null != (row = this.csvReader.readNext()) && this.csvReader.getLinesRead() < lastOffset) {
        LOGGER.debug("skipped row");
      }
    }
    this.fieldNames = fieldNames;
    this.fileMetadata = metadata;
  }

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);
  }

  @Override
  public long recordOffset() {
    return this.csvReader.getLinesRead();
  }

  @Override
  public List<SourceRecord> process() throws IOException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);
    while (records.size() < this.config.batchSize) {
      String[] row = this.csvReader.readNext();
      if (row == null) {
        break;
      }
      LOGGER.debug("process() - Row on line {} has {} field(s)", recordOffset(), row.length);

      final Struct keyStruct = new Struct(this.config.keySchema);
      final Field keyField = this.config.keySchema.field("uuid");
      keyStruct.put(keyField, java.util.UUID.randomUUID().toString());
      try {
        final Struct valueStruct = sourceRecords(row, this.fieldNames, this.config.valueSchema);
        if (LOGGER.isInfoEnabled() && this.csvReader.getLinesRead() % ((long) this.config.batchSize * 20) == 0) {
          LOGGER.info("Processed {} lines of {}", this.csvReader.getLinesRead(), this.fileMetadata);
        }
        addRecord(records, keyStruct, valueStruct);
      } catch (Exception ex) {
        LOGGER.warn("Error in parsing recrods :[{}], \n fieldnames:[{}] ", Arrays.asList(row), this.fieldNames);
        final Struct errorStruct = sourceErrorRecord(row, this.fieldNames, this.config.errorPayloadSchema);
        addRecord(records, keyStruct, errorStruct, "error");
      }
    }
    LOGGER.info("===> batch fouund :[{}]", records.size());
    return records;
  }

  public Struct sourceErrorRecord(final String[] arrayOfValues, final String[] fieldNames, final org.apache.kafka.connect.data.Schema _errorSchema) {
    final Struct errorStruct = new Struct(_errorSchema);
    final Field keyField = _errorSchema.field("uuid");
    errorStruct.put(keyField, java.util.UUID.randomUUID().toString());

    final Field topicField = _errorSchema.field("topic");
    errorStruct.put(topicField, this.config.topic);
    
    final Map<String, String> valueMap = new HashMap<>();
    String errorPayload = "";
    try {
      for (int i = 0; i < this.fieldNames.length; i++) {
        final String fieldName = this.fieldNames[i];
        final String input = arrayOfValues[i];
        valueMap.put(fieldName, input);
        errorPayload = SpooldirUtil.jsonMapper().writeValueAsString(valueMap);
      }
    } catch (final Exception ex) {
      LOGGER.error("Error in converting error payload : [{}]", errorPayload);
    }
    
    final Field payloadField = _errorSchema.field("payload"); 
    errorStruct.put(payloadField, errorPayload);
    return errorStruct;
  }

  public Struct sourceRecords(final String[] arrayOfValues, final String[] fieldNames, final org.apache.kafka.connect.data.Schema _valueSchema) {
    final Struct valueStruct = new Struct(_valueSchema);

    for (int i = 0; i < this.fieldNames.length; i++) {
      final String fieldName = this.fieldNames[i];
      final String input = arrayOfValues[i];

      Object fieldValue = null;
      Field field = _valueSchema.field(fieldName);
      if (null != field) {
        fieldValue = this.parser.parseString(field.schema(), input);
        valueStruct.put(field, fieldValue);
      } else {
        LOGGER.info("process() - Field {} is not defined in the schema.", fieldName);
      }
    }
    return valueStruct;
  }

}


