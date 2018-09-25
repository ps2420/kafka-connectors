package com.amaris.kafka.connect.spooldir;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestCase implements NamedTest {
  @JsonIgnore
  public Path path;
  public Map<String, String> settings = new LinkedHashMap<>();
  public Map<String, Object> offset = new LinkedHashMap<>();
  public Schema keySchema;
  public Schema valueSchema;
  
  @JsonIgnore
  public List<SourceRecord> expected;

  @Override
  public void path(Path path) {
    this.path = path;
  }
}
