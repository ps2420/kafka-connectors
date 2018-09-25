package com.amaris.kafka.connect.spooldir;

import com.amaris.kafka.connect.spooldir.SpoolDirJsonSourceConnector;
import com.amaris.kafka.connect.spooldir.SpoolDirJsonSourceConnectorConfig;
import com.google.common.io.ByteStreams;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SpoolDirJsonSourceConnectorTest extends SpoolDirSourceConnectorTest<SpoolDirJsonSourceConnector> {
  @Override
  protected SpoolDirJsonSourceConnector createConnector() {
    return new SpoolDirJsonSourceConnector();
  }

  @Test
  public void startWithoutSchema() throws IOException {
    settings.put(SpoolDirJsonSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.*\\.json$");

    String[] inputFiles = new String[]{
        "json/FieldsMatch.data",
        "json/FieldsMatch.data",
    };

    int index = 0;
    for (String inputFile : inputFiles) {
      try (InputStream inputStream = this.getClass().getResourceAsStream(inputFile)) {
        File outputFile = new File(this.inputPath, "input" + index + ".json");
        try (OutputStream outputStream = new FileOutputStream(outputFile)) {
          ByteStreams.copy(inputStream, outputStream);
        }
      }
      index++;
    }

    this.connector.start(settings);
  }

  @Test()
  public void startWithoutSchemaMismatch() throws IOException {
    this.settings.put(SpoolDirJsonSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.*\\.json$");


    String[] inputFiles = new String[]{
        "json/FieldsMatch.data",
        "json/DataHasMoreFields.data",
    };

    int index = 0;
    for (String inputFile : inputFiles) {
      try (InputStream inputStream = this.getClass().getResourceAsStream(inputFile)) {
        File outputFile = new File(this.inputPath, "input" + index + ".json");
        try (OutputStream outputStream = new FileOutputStream(outputFile)) {
          ByteStreams.copy(inputStream, outputStream);
        }
      }
      index++;
    }

    assertThrows(DataException.class, () -> {
      this.connector.start(settings);
    });

  }
}
