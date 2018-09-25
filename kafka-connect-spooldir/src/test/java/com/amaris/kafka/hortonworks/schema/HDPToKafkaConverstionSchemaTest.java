package com.amaris.kafka.hortonworks.schema;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amaris.kafka.connect.hdp.schema.HDPSchema;
import com.amaris.kafka.connect.hdp.schema.HDPToKafkaSchemaConvertor;

public class HDPToKafkaConverstionSchemaTest {

  private static Logger LOGGER = LoggerFactory.getLogger(HDPToKafkaConverstionSchemaTest.class);

  public static void main(String[] args) throws Exception {
    final HDPSchema hdpSchema = HDPToKafkaSchemaConvertor.getHortonWorksSchema("");
    final Map<String, Object> connectSchemaHeader = HDPToKafkaSchemaConvertor.connectSchemaHeader(hdpSchema);
    final String connectSchema = HDPToKafkaSchemaConvertor.enrichConnectSchema(connectSchemaHeader, hdpSchema);
    LOGGER.info("connectSchema :\n{}", connectSchema);
  }
}
