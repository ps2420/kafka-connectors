package com.memorynotfound.cloud;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ClassicKafkaProducer {

  private final static String BOOTSTRAP_SERVERS = "192.168.1.119:6667,192.168.1.35:6667"; // 192.168.1.119
  private final static String TOPIC = "file_upload_event"; // "t-address-es-input";

  private static KafkaProducer<String, String> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    return new KafkaProducer<>(props);
  }

  public static void main(String... args) {
    final KafkaProducer<String, String> producer = createProducer();
    producer.send(new ProducerRecord(TOPIC, 0, java.util.UUID.randomUUID().toString(), java.util.UUID.randomUUID().toString()));
    producer.close(); 
  }
}
