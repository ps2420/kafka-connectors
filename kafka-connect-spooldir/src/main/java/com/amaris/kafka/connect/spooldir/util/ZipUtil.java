package com.amaris.kafka.connect.spooldir.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZipUtil.class);

  public static void zipOutputDirFile(final File inputfile) {
    final String filename = inputfile.getName();
    String outputfile = filename + "_" + System.currentTimeMillis() + ".zip";
    
    if (filename.lastIndexOf(".") != -1) {
      final String file_extension = filename.substring(filename.lastIndexOf("."), filename.length());
      final String file_prefix = filename.substring(0, filename.lastIndexOf("."));
      final String renamedfile = inputfile.getParent() + "/" + file_prefix + "_" + System.currentTimeMillis() + file_extension;

      final File rename_file = new File(renamedfile);
      inputfile.renameTo(rename_file);

      LOGGER.info("Input file:[{}] is being renamed to file:[{}]", inputfile.getName(), rename_file.getName());

      outputfile = file_prefix + "_" + System.currentTimeMillis() + file_extension + ".zip";
      zipSingleFile(rename_file, inputfile.getParent() + "/" + outputfile);
    }
  }

  private static void zipSingleFile(final File file, final String zipFileName) {
    try (final FileOutputStream fos = new FileOutputStream(zipFileName);
        final ZipOutputStream zos = new ZipOutputStream(fos);
        final FileInputStream fis = new FileInputStream(file);) {

      final ZipEntry ze = new ZipEntry(file.getName());
      zos.putNextEntry(ze);

      byte[] buffer = new byte[1024];
      int len;
      while ((len = fis.read(buffer)) > 0) {
        zos.write(buffer, 0, len);
      }
      zos.closeEntry();
      LOGGER.info(file.getCanonicalPath() + " is zipped to " + zipFileName);

    } catch (final IOException ex) {
      LOGGER.warn("Error in zipping the file : " + file.getName());
      LOGGER.error("Error in zipping the file : " + ex, ex);
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }
}
