package com.amaris.kafka.connect.spooldir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amaris.kafka.connect.spooldir.util.SpooldirUtil;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public abstract class SpoolDirSourceTaskTest<T extends SpoolDirSourceTask> {
  
  private static final Logger log = LoggerFactory.getLogger(SpoolDirSourceTaskTest.class);

  protected File tempDirectory;
  protected File inputPath;
  protected File errorPath;
  protected File finishedPath;
  protected T task;

  @BeforeEach
  public void setup() {
    this.tempDirectory = Files.createTempDir();
    this.finishedPath = new File(this.tempDirectory, "finished");
    this.inputPath = new File(this.tempDirectory, "input");
    this.errorPath = new File(this.tempDirectory, "error");
    this.finishedPath.mkdirs();
    this.inputPath.mkdirs();
    this.errorPath.mkdirs();
  }

  @BeforeEach
  public void configureIndent() {
    ObjectMapperFactory.INSTANCE.configure(SerializationFeature.INDENT_OUTPUT, true);
  }

  protected abstract T createTask();

  protected abstract void settings(Map<String, String> settings);

  protected void poll(final String packageName, TestCase testCase) throws InterruptedException, IOException {
    String keySchemaConfig = ObjectMapperFactory.INSTANCE.writeValueAsString(testCase.keySchema);
    String valueSchemaConfig = ObjectMapperFactory.INSTANCE.writeValueAsString(testCase.valueSchema);

    Map<String, String> settings = Maps.newLinkedHashMap();
    settings.put(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, this.inputPath.getAbsolutePath());
    settings.put(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, this.finishedPath.getAbsolutePath());
    settings.put(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, this.errorPath.getAbsolutePath());
    settings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, String.format("^.*\\.%s", packageName));
    settings.put(SpoolDirSourceConnectorConfig.TOPIC_CONF, "testing");
    settings.put(SpoolDirSourceConnectorConfig.TOPIC_ERROR_CONF, "testing");
    settings.put(SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, keySchemaConfig);
    settings.put(SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, valueSchemaConfig);
    settings.put(SpoolDirSourceConnectorConfig.EMPTY_POLL_WAIT_MS_CONF, "10");
    settings.put(SpoolDirSourceConnectorConfig.FILE_AUDIT_TOPIC_CONF, "TEST");
    settings.put(SpoolDirSourceConnectorConfig.FILE_AUDIT_KEY_SCHEMA_CONF, SpooldirUtil.loadKafkaKeySchema("file_audit_key.schema"));
    settings.put(SpoolDirSourceConnectorConfig.FILE_AUDIT_VALUE_SCHEMA_CONF, SpooldirUtil.loadKafkaKeySchema("file_audit_value.schema"));
    settings.put(SpoolDirSourceConnectorConfig.SCHEMA_REGISTRY_URL, "http://localhost:9001/api/v1");
    
    settings(settings);
    if (null != testCase.settings && !testCase.settings.isEmpty()) {
      settings.putAll(testCase.settings);
    }

    this.task = createTask();

    SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    when(offsetStorageReader.offset(anyMap())).thenReturn(testCase.offset);
    when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
    this.task.initialize(sourceTaskContext);

    this.task.start(settings);

    String dataFile = new File(packageName, Files.getNameWithoutExtension(testCase.path.toString())) + ".data";
    log.trace("poll(String, TestCase) - dataFile={}", dataFile);

    String inputFileName = String.format("%s.%s", Files.getNameWithoutExtension(testCase.path.toString()), packageName);


    final File inputFile = new File(this.inputPath, inputFileName);
    log.trace("poll(String, TestCase) - inputFile = {}", inputFile);
    final File processingFile = InputFileDequeue.processingFile(SpoolDirSourceConnectorConfig.PROCESSING_FILE_EXTENSION_DEFAULT, inputFile);
    try (InputStream inputStream = this.getClass().getResourceAsStream(dataFile)) {
      try (OutputStream outputStream = new FileOutputStream(inputFile)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    assertFalse(processingFile.exists(), String.format("processingFile %s should not exist before first poll().", processingFile));
    assertTrue(inputFile.exists(), String.format("inputFile %s should exist.", inputFile));
    List<SourceRecord> records = this.task.poll();
    assertTrue(inputFile.exists(), String.format("inputFile %s should exist after first poll().", inputFile));
    assertTrue(processingFile.exists(), String.format("processingFile %s should exist after first poll().", processingFile));

    assertNotNull(records, "records should not be null.");
    assertFalse(records.isEmpty(), "records should not be empty");
    assertEquals(testCase.expected.size(), records.size(), "records.size() does not match.");

    for (int i = 0; i < testCase.expected.size(); i++) {
      SourceRecord expectedRecord = testCase.expected.get(i);
      SourceRecord actualRecord = records.get(i);
    }

    records = this.task.poll();
    assertTrue(records.isEmpty(), "records should be null after first poll.");
    records = this.task.poll();
    assertTrue(records.isEmpty(), "records should be null after first poll.");
    assertFalse(inputFile.exists(), String.format("inputFile %s should not exist.", inputFile));
    assertFalse(processingFile.exists(), String.format("processingFile %s should not exist.", processingFile));

    assertTrue(records.isEmpty(), "records should be empty.");

    final File finishedFile = new File(this.finishedPath, inputFileName);
    assertTrue(finishedFile.exists(), String.format("finishedFile %s should exist.", finishedFile));

  }

  protected List<TestCase> loadTestCases(String packageName) throws IOException {
    String packagePrefix = String.format("%s.%s", this.getClass().getPackage().getName(), packageName);
    log.trace("packagePrefix = {}", packagePrefix);
    List<TestCase> testCases = TestDataUtils.loadJsonResourceFiles(packagePrefix, TestCase.class);
    if (testCases.isEmpty() && log.isWarnEnabled()) {
      log.warn("No test cases were found in the resources. packagePrefix = {}", packagePrefix);
    }
    return testCases;
  }

  @Test
  public void version() {
    this.task = createTask();
    assertNotNull(this.task.version(), "version should not be null.");
  }

  @AfterEach
  public void after() {
    if (null != this.task) {
      this.task.stop();
    }
  }
}
