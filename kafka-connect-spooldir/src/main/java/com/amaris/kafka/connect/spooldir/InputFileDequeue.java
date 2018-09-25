
package com.amaris.kafka.connect.spooldir;

import com.google.common.collect.ForwardingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

public class InputFileDequeue extends ForwardingDeque<File> {

  private static final Logger LOGGER = LoggerFactory.getLogger(InputFileDequeue.class);

  private final SpoolDirSourceConnectorConfig config;

  public InputFileDequeue(SpoolDirSourceConnectorConfig config) {
    this.config = config;
  }

  public static File processingFile(String processingFileExtension, File input) {
    String fileName = input.getName() + processingFileExtension;
    return new File(input.getParentFile(), fileName);
  }

  Deque<File> files;

  @Override
  protected Deque<File> delegate() {
    if (null != files && !files.isEmpty()) {
      return files;
    }

    LOGGER.debug("Searching for file in {}", this.config.inputPath);
    File[] input = this.config.inputPath.listFiles(this.config.inputFilenameFilter);
    if (null == input || input.length == 0) {
      LOGGER.debug("No files matching {} were found in {}", SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, this.config.inputPath);
      return new ArrayDeque<>();
    }

    Arrays.sort(input, Comparator.comparing(File::getName));
    List<File> files = new ArrayList<>(input.length);

    for (File f : input) {
      File processingFile = processingFile(this.config.processingFileExtension, f);
      LOGGER.trace("Checking for processing file: {}", processingFile);

      if (processingFile.exists()) {
        LOGGER.debug("Skipping {} because processing file exists.", f);
        continue;
      }
      files.add(f);
    }

    Deque<File> result = new ArrayDeque<>(files.size());

    for (File file : files) {
      long fileAgeMS = System.currentTimeMillis() - file.lastModified();
      if (fileAgeMS < 0L) {
        LOGGER.warn("File {} has a date in the future.", file);
      }
      if (this.config.minimumFileAgeMS > 0L && fileAgeMS < this.config.minimumFileAgeMS) {
        LOGGER.debug("Skipping {} because it does not meet the minimum age.", file);
        continue;
      }
      result.add(file);
    }
    final List<String> filenames = result.stream().map(file -> file.getName()).collect(Collectors.toList());
    LOGGER.info("===> Found {} file(s) to be processed size:[{}], names :[{}]", result.size(), filenames);
    return (this.files = result);
  }
}
