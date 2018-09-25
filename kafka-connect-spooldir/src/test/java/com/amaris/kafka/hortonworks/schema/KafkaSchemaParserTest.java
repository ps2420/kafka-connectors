package com.amaris.kafka.hortonworks.schema;

import com.amaris.kafka.connect.spooldir.util.SpooldirUtil;

public class KafkaSchemaParserTest {

  public static void main(String[] args) throws Exception{
    final String keySchema = SpooldirUtil.loadKafkaKeySchema("address_key.schema");
    System.out.println(keySchema);
  }

}
