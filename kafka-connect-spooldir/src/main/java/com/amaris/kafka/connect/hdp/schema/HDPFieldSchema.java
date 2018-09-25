package com.amaris.kafka.connect.hdp.schema;

import java.io.Serializable;

public class HDPFieldSchema implements Serializable {

  private static final long serialVersionUID = 1L;
 
  private String name;
  private String type;
  private boolean isOptional;
  private String defaultValue;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public boolean isOptional() {
    return isOptional;
  }

  public void setOptional(boolean isOptional) {
    this.isOptional = isOptional;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }



}
