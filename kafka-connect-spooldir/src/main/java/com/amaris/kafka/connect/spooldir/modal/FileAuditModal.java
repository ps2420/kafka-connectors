package com.amaris.kafka.connect.spooldir.modal;

import java.io.Serializable;

public class FileAuditModal implements Serializable {

  private static final long serialVersionUID = 1L;
  private String uuid;
  private String filename;
  private String event;

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public String getEvent() {
    return event;
  }

  public void setEvent(String event) {
    this.event = event;
  }
}
