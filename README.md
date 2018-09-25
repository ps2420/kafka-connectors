
 # Welcome to kafka-connectos and installation

## Installation location
```
........../confluent-4.1.1
```

## Kafka connect properties change
```
cd ........../confluent-4.1.1/etc/kafka
edit connect-distributed.properties & connect-standalone.properties and change the value to false
key.converter.schemas.enable=false
value.converter.schemas.enable=false

```

## Kafka start command:
```
cd ........../confluent-4.1.1
 kafka-server.  : bin/kafka-server-start etc/kafka/server.properties 
 
 kafka-connect  : bin/connect-distributed etc/kafka/connect-distributed.properties 
 
 schema-registry: horton works schema registry will be used
                  bin/schema-registry-start etc/schema-registry/schema-registry.properties

 zoookeeper     : hortown works zookeeper will be used
                  bin/zookeeper-server-start etc/kafka/zookeeper.properties
```

## Kafka stop command:
```
cd ........../confluent-4.1.1
 kafka-server.  : bin/kafka-server-stop.  (or ps -aef | grep java | grep kafka | grep server.properties)
 
 kafka-connect  : ps -aef | grep java | grep kafka | grep connect 
 
 schema-registry: horton works schema registry will be used (/usr/hdp/..) 

 zoookeeper     : hortown works zookeeper will be used (/usr/hdp/..)
```

## kafka tool commands
```
look for kafka_tool_commands_1.jpg inside attachments..
```

## Kafka topic create/list/delete commands
```
cd ........../confluent-4.1.1
bin/kafka-topics --list --zookeeper localhost:2181
bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --config retention.ms=1440000000 --topic <topic-name>
bin/kafka-topics --zookeeper localhost:2181 --delete --topic <topic-name>

Adding topic retention message time
bin/kafka-configs --zookeeper localhost:2181 --alter --entity-type topics --entity-name <topic-name> --add-config retention.ms=86400000
```

## message produce and consume example
```
cd ........../confluent-4.1.1
bin/kafka-console-producer --broker-list localhost:9092 --topic <topic-name>
bin/kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --topic <topic-name>  
```

## Create connectors by res command
```
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors --data '{"name": "kafka-spool-dir", "config": {"tasks.max" : "1","connector.class" : "com.amaris.kafka.connect.spooldir.SpoolDirCsvSourceConnector","finished.path" : "/users/pankaj.pankasin/softwares/kafka/kafka-connect-example/output","input.file.pattern" : "test6.csv$","error.path" : "/users/pankaj.pankasin/softwares/kafka/kafka-connect-example/error","topic" : "test","halt.on.error" : "false","csv.first.row.as.header" : "true", "schema.generation.enabled" : "true", "input.path" : "/users/pankaj.pankasin/softwares/kafka/kafka-connect-example/input" }}' 
```






