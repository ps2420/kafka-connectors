package com.memorynotfound.cloud;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ClassicKafkaConsumer {

  private final static String BOOTSTRAP_SERVERS = "192.168.1.123:6667,192.168.1.119:6667,192.168.1.35:6667"; //192.168.1.119
  private final static String TOPIC = "test-tofile_upload_event"; // "t-address-es-input";

  private static Consumer<String, Map<String, String>> createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.connect.json.JsonDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
     
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(props);
  }

  public static void main(String... args) {
    final Consumer<String, Map<String, String>> consumer = createConsumer();
    consumer.subscribe(Collections.singletonList(TOPIC.toLowerCase()));

    final AtomicInteger tracker = new AtomicInteger(0);
    IntStream.range(1, 1000).forEach(index -> {
      final ConsumerRecords<String, Map<String, String>> records = consumer.poll(100);
      tracker.set(tracker.get() + records.count());
      System.out.println("Total records read : " + tracker.get());
      if(records.count() > 0) {
        records.forEach(record -> {
          System.out.println(record.value());
        });
      }
      try {
        Thread.sleep(3000l);
      }catch(final Exception ex) {}
    });
    consumer.close();
    System.out.println("Total records read from kafka topic :[{}]" + tracker.get());
  }
}
