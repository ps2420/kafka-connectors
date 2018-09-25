package com.amaris.kafka.connect.spooldir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.utils.jackson.HeaderSerializationModule;
import com.github.jcustenborder.kafka.connect.utils.jackson.SchemaSerializationModule;
import com.github.jcustenborder.kafka.connect.utils.jackson.SinkRecordSerializationModule;
import com.github.jcustenborder.kafka.connect.utils.jackson.SourceRecordSerializationModule;
import com.github.jcustenborder.kafka.connect.utils.jackson.StructSerializationModule;
import com.github.jcustenborder.kafka.connect.utils.jackson.TimeSerializationModule;
import com.google.common.base.Preconditions;

public class TestDataUtils {
  private static final Logger log = LoggerFactory.getLogger(TestDataUtils.class);

  public static <T extends NamedTest> List<T> loadJsonResourceFiles(String packageName, Class<T> cls) throws IOException {
    Preconditions.checkNotNull(packageName, "packageName cannot be null");
    log.info("packageName = {}", packageName);
//    Preconditions.checkState(packageName.startsWith("/"), "packageName must start with a /.");
    Reflections reflections = new Reflections(packageName, new ResourcesScanner());
    Set<String> resources = reflections.getResources(new FilterBuilder.Include("^.*\\.json$"));
    List<T> datas = new ArrayList<T>(resources.size());
    Path packagePath = Paths.get("/" + packageName.replace(".", "/"));
    for (String resource : resources) {
      log.info("Loading resource {}", resource);
      Path resourcePath = Paths.get("/" + resource);
      Path relativePath = packagePath.relativize(resourcePath);
      File resourceFile = new File("/" + resource);
      T data;
      try (InputStream inputStream = cls.getResourceAsStream(resourceFile.getAbsolutePath())) {
        //System.out.println(IOUtils.toString(inputStream));
        data = getObjectMapper().readValue(inputStream, cls);
      } catch (IOException ex) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown while loading {}", resourcePath, ex);
        }
        throw ex;
      }

      if (null != relativePath.getParent()) {
        data.path(relativePath);
      } else {
        data.path(relativePath);
      }
      datas.add(data);
    }
    return datas;
  }
  
  public static ObjectMapper getObjectMapper() {
    ObjectMapper INSTANCE = new ObjectMapper();
    INSTANCE.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    INSTANCE.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
    INSTANCE.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
    INSTANCE.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);

    INSTANCE.registerModule(new SchemaSerializationModule());
    INSTANCE.registerModule(new StructSerializationModule());
    INSTANCE.registerModule(new SinkRecordSerializationModule());
    INSTANCE.registerModule(new SourceRecordSerializationModule());
    INSTANCE.registerModule(new TimeSerializationModule());
    INSTANCE.registerModule(new HeaderSerializationModule());
    return INSTANCE;
  }
}
