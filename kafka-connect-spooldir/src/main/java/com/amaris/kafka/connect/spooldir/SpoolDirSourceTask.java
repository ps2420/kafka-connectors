
package com.amaris.kafka.connect.spooldir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amaris.kafka.connect.spooldir.util.SpooldirUtil;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.github.jcustenborder.kafka.connect.utils.data.type.DateTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimeTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimestampTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TypeParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import reactor.util.function.Tuple2;

public abstract class SpoolDirSourceTask<CONF extends SpoolDirSourceConnectorConfig> extends SourceTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpoolDirSourceTask.class);

  protected Parser parser;
  protected Map<String, ?> sourcePartition;
  protected CONF config;

  private Stopwatch processingTime = Stopwatch.createStarted();
  private File inputFile;
  private long inputFileModifiedTime;
  private InputStream inputStream;
  private boolean hasRecords = false;
  private Map<String, String> metadata;

  private static void checkDirectory(String key, File directoryPath) {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Checking if directory {} '{}' exists.", key, directoryPath);
    }

    String errorMessage = String.format("Directory for '%s' '%s' does not exist ", key, directoryPath);
    if (!directoryPath.isDirectory()) {
      throw new ConnectException(errorMessage, new FileNotFoundException(directoryPath.getAbsolutePath()));
    }

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Checking to ensure {} '{}' is writable ", key, directoryPath);
    }

    errorMessage = String.format("Directory for '%s' '%s' it not writable.", key, directoryPath);
    File temporaryFile = null;

    try {
      temporaryFile = File.createTempFile(".permission", ".testing", directoryPath);
    } catch (IOException ex) {
      throw new ConnectException(errorMessage, ex);
    } finally {
      try {
        if (null != temporaryFile && temporaryFile.exists()) {
          Preconditions.checkState(temporaryFile.delete(), "Unable to delete temp file in %s", directoryPath);
        }
      } catch (Exception ex) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Exception thrown while deleting {}.", temporaryFile, ex);
        }
      }
    }
  }

  protected abstract CONF config(Map<String, ?> settings);

  protected abstract void configure(InputStream inputStream, Map<String, String> metadata, Long lastOffset) throws IOException;

  protected abstract List<SourceRecord> process() throws IOException;

  protected abstract long recordOffset();

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  InputFileDequeue inputFileDequeue;

  @Override
  public void start(Map<String, String> settings) {
    this.config = config(settings);

    checkDirectory(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, this.config.inputPath);
    checkDirectory(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, this.config.errorPath);

    if (SpoolDirSourceConnectorConfig.CleanupPolicy.MOVE == this.config.cleanupPolicy) {
      checkDirectory(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, this.config.finishedPath);
    }

    this.parser = new Parser();
    
    Map<Schema, TypeParser> dateTypeParsers =
        ImmutableMap.of(Timestamp.SCHEMA, new TimestampTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
            Date.SCHEMA, new DateTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats), Time.SCHEMA,
            new TimeTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats));

    for (Map.Entry<Schema, TypeParser> kvp : dateTypeParsers.entrySet()) {
      this.parser.registerTypeParser(kvp.getKey(), kvp.getValue());
    }

    this.inputFileDequeue = new InputFileDequeue(this.config);
  }

  @Override
  public void stop() {}

  int emptyCount = 0;

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> results = read();
    if (results.isEmpty()) {
      emptyCount++;
      if (emptyCount > 1) {
        LOGGER.debug("read() returned empty list. Sleeping {} ms.", this.config.emptyPollWaitMs);
        Thread.sleep(this.config.emptyPollWaitMs);
      }
      return results;
    }
    emptyCount = 0;
    return results;
  }

  private void recordProcessingTime() {
    LOGGER.info("Finished processing {} record(s) in {} second(s).", this.recordCount, processingTime.elapsed(TimeUnit.SECONDS));
  }

  private void closeAndMoveToFinished(File outputDirectory, boolean errored) throws IOException {
    if (null != inputStream) {
      LOGGER.info("Closing {}", this.inputFile);

      this.inputStream.close();
      this.inputStream = null;

      File finishedFile = new File(outputDirectory, this.inputFile.getName());

      if (errored) {
        LOGGER.error("Error during processing, moving {} to {}.", this.inputFile, outputDirectory);
      } else {
        recordProcessingTime();
        LOGGER.info("Moving inputfile :[{}] to directory:[{}].", this.inputFile, outputDirectory);
      }

      Files.move(this.inputFile, finishedFile);

      File processingFile = InputFileDequeue.processingFile(this.config.processingFileExtension, this.inputFile);
      if (processingFile.exists()) {
        LOGGER.info("Removing processing file {}", processingFile);
        processingFile.delete();
      }
      com.amaris.kafka.connect.spooldir.util.ZipUtil.zipOutputDirFile(finishedFile); // zip the file in output dir
    }
  }

  static final Map<String, String> SUPPORTED_COMPRESSION_TYPES =
      ImmutableMap.of("bz2", CompressorStreamFactory.BZIP2, "gz", CompressorStreamFactory.GZIP, "snappy", CompressorStreamFactory.SNAPPY_RAW, "lz4",
          CompressorStreamFactory.LZ4_BLOCK, "z", CompressorStreamFactory.Z);


  public List<SourceRecord> read() {
    try {
      SourceRecord inputFileSourceRecord = null;
      if (!hasRecords) {
        switch (this.config.cleanupPolicy) {
          case MOVE:
            closeAndMoveToFinished(this.config.finishedPath, false);
            break;
          case DELETE:
            closeAndDelete();
            break;
        }
        File nextFile = this.inputFileDequeue.poll();
        if (null == nextFile) {
          return new ArrayList<>();
        }

        this.metadata = ImmutableMap.of();
        this.inputFile = nextFile;
        this.inputFileModifiedTime = this.inputFile.lastModified();

        File processingFile = InputFileDequeue.processingFile(this.config.processingFileExtension, this.inputFile);
        Files.touch(processingFile);

        try {
          this.sourcePartition = ImmutableMap.of("fileName", this.inputFile.getName());
          LOGGER.info("Opening {}", this.inputFile);

          Long lastOffset = null;
          LOGGER.trace("looking up offset for {}", this.sourcePartition);

          Map<String, Object> offset = this.context.offsetStorageReader().offset(this.sourcePartition);
          if (null != offset && !offset.isEmpty()) {
            Number number = (Number) offset.get("offset");
            lastOffset = number.longValue();
          }

          final String extension = Files.getFileExtension(inputFile.getName());
          LOGGER.trace("read() - fileName = '{}' extension = '{}'", inputFile, extension);
          final InputStream inputStream = new FileInputStream(this.inputFile);

          if (SUPPORTED_COMPRESSION_TYPES.containsKey(extension)) {
            final String compressor = SUPPORTED_COMPRESSION_TYPES.get(extension);
            LOGGER.info("Decompressing {} as {}", inputFile, compressor);
            final CompressorStreamFactory compressorStreamFactory = new CompressorStreamFactory();
            this.inputStream = compressorStreamFactory.createCompressorInputStream(compressor, inputStream);
          } else {
            this.inputStream = inputStream;
          }
          this.recordCount = 0;
          configure(this.inputStream, this.metadata, lastOffset);
        } catch (Exception ex) {
          throw new ConnectException(ex);
        }
        processingTime.reset();
        processingTime.start();

        if (this.inputFile != null) {
          LOGGER.info("===> Processing new file: [{}], constructing new Source Record ", this.inputFile.getName());
          inputFileSourceRecord = getFileAuditSourceRecord("0", "START");
        }
      }
      List<SourceRecord> records = process();
      if (inputFileSourceRecord != null) {
        records.add(inputFileSourceRecord);
      }

      this.hasRecords = !records.isEmpty();
      if (!hasRecords && this.inputFile != null) {
        LOGGER.info("===> File processing is ending now : [{}], constructing new source record", this.inputFile);
        final SourceRecord endEventRecord = getFileAuditSourceRecord("1", "END");
        if (endEventRecord != null) {
          records.add(endEventRecord);
        }
      }
      return records;
    } catch (Exception ex) {
      LOGGER.error("Exception encountered processing line {} of {}.", recordOffset(), this.inputFile, ex);

      try {
        closeAndMoveToFinished(this.config.errorPath, true);
      } catch (IOException ex0) {
        LOGGER.error("Exception thrown while moving {} to {}", this.inputFile, this.config.errorPath, ex0);
      }
      if (this.config.haltOnError) {
        throw new ConnectException(ex);
      } else {
        return new ArrayList<>();
      }
    }
  }

  private SourceRecord getFileAuditSourceRecord(final String offset, final String event) {
    try {
      Tuple2<Struct, Struct> tuple2 =
          SpooldirUtil.getFileAuditEvent(this.config.file_audit_keySchema, this.config.file_audit_valueSchema, this.inputFile.getName(), event);

      return new SourceRecord(ImmutableMap.of("fileName", this.inputFile.getName() + "fileaudit"), ImmutableMap.of("offset", offset),
          this.config.file_audit_topic, null, this.config.file_audit_keySchema, tuple2.getT1(), this.config.file_audit_valueSchema, tuple2.getT2(),
          System.currentTimeMillis());
    } catch (final Exception ex) {
      LOGGER.error("Erorr in construction source record : " + ex, ex);
    }
    return null;
  }

  private void closeAndDelete() throws IOException {
    if (null != inputStream) {
      LOGGER.info("Closing {}", this.inputFile);
      this.inputStream.close();
      this.inputStream = null;
      recordProcessingTime();
      LOGGER.info("Removing file {}", this.inputFile);
      this.inputFile.delete();
      File processingFile = InputFileDequeue.processingFile(this.config.processingFileExtension, this.inputFile);
      if (processingFile.exists()) {
        LOGGER.info("Removing processing file {}", processingFile);
        processingFile.delete();
      }
    }
  }

  long recordCount;

  protected void addRecord(final List<SourceRecord> records, final Struct keyStruct, final Struct valueStruct) {
    addRecord(records, keyStruct, valueStruct, null);
  }

  protected void addRecord(final List<SourceRecord> records, final Struct keyStruct, final Struct valueStruct, final String type) {
    Map<String, ?> sourceOffset = ImmutableMap.of("offset", recordOffset());
    LOGGER.debug("addRecord() - {}", sourceOffset);
    if (this.config.hasKeyMetadataField && null != keyStruct) {
      keyStruct.put(this.config.keyMetadataField, this.metadata);
    }

    if (this.config.hasvalueMetadataField && null != valueStruct) {
      valueStruct.put(this.config.valueMetadataField, this.metadata);
    }

    final Long timestamp;

    switch (this.config.timestampMode) {
      case FIELD:
        LOGGER.debug("addRecord() - Reading date from timestamp field '{}'", this.config.timestampField);
        java.util.Date date = (java.util.Date) valueStruct.get(this.config.timestampField);
        timestamp = date.getTime();
        break;
      case FILE_TIME:
        timestamp = this.inputFileModifiedTime;
        break;
      case PROCESS_TIME:
        timestamp = null;
        break;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported timestamp mode. %s", this.config.timestampMode));
    }

    String _topic_name = this.config.topic;
    if (!StringUtils.isEmpty(type) && "error".equalsIgnoreCase(type)) {
      _topic_name = this.config.error_topic;
    }

    final SourceRecord sourceRecord = new SourceRecord(this.sourcePartition, sourceOffset, _topic_name, null,
        null != keyStruct ? keyStruct.schema() : null, keyStruct, valueStruct.schema(), valueStruct, timestamp);

    recordCount++;
    LOGGER.debug("===========>Total records where added: [{}]", recordCount);
    records.add(sourceRecord);
  }
}
