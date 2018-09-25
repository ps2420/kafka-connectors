package com.amaris.kafka.connect.hdp.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class HDPSchema implements Serializable {

  private static final long serialVersionUID = 1L;
  private String type;
  private String namespace;
  private String name;

  private List<HDPFieldSchema> fields = new ArrayList<>();

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<HDPFieldSchema> getFields() {
    return fields;
  }

  public void setFields(List<HDPFieldSchema> fields) {
    this.fields = fields;
  }
}
