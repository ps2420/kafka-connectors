package com.amaris.kafka.connect.spooldir;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SpoolDirSourceConnectorTest<T extends SpoolDirSourceConnector> {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(SpoolDirSourceConnectorTest.class);
  
  protected T connector;
  protected Map<String, String> settings;
  
  File tempRoot;
  File inputPath;
  File finishedPath;
  File errorPath;

  protected abstract T createConnector();

  @BeforeEach
  public void before() {
    this.connector = createConnector();
  }

  @Test
  public void taskClass() {
    assertNotNull(this.connector.taskClass());
  }

  @BeforeEach
  public void createTempDir() {
    this.tempRoot = new File("/users/pankaj.pankasin/softwares/kafka/kafka-connect-example");//Files.createTempDir();
    this.inputPath = new File(this.tempRoot, "input");
    //this.inputPath.mkdirs();
    this.finishedPath = new File(this.tempRoot, "finished");
    //this.finishedPath.mkdirs();
    this.errorPath = new File(this.tempRoot, "error");
    //this.errorPath.mkdirs();

    this.settings = new LinkedHashMap<>();
    this.settings.put(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, this.inputPath.getAbsolutePath());
    this.settings.put(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, this.finishedPath.getAbsolutePath());
    this.settings.put(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, this.errorPath.getAbsolutePath());
    this.settings.put(SpoolDirSourceConnectorConfig.TOPIC_CONF, "dummy");
    this.settings.put(SpoolDirSourceConnectorConfig.TOPIC_ERROR_CONF, "dummy");
    this.settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");
  }

  //@AfterEach
  public void cleanupTempDir() throws IOException {
    java.nio.file.Files.walkFileTree(this.tempRoot.toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        LOGGER.trace("cleanupTempDir() - Removing {}", file);
        java.nio.file.Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        LOGGER.trace("cleanupTempDir() - Removing {}", file);
        java.nio.file.Files.delete(file);
        return FileVisitResult.CONTINUE;
      }
    });
  }


}
