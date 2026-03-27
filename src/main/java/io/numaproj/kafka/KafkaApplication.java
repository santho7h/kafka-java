package io.numaproj.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.numaproj.kafka.config.ConsumerConfig;
import io.numaproj.kafka.config.ProducerConfig;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.kafka.consumer.Admin;
import io.numaproj.kafka.consumer.AvroSourcer;
import io.numaproj.kafka.consumer.AvroWorker;
import io.numaproj.kafka.consumer.ByteArraySourcer;
import io.numaproj.kafka.consumer.ByteArrayWorker;
import io.numaproj.kafka.producer.KafkaAvroSinker;
import io.numaproj.kafka.producer.KafkaByteArraySinker;
import io.numaproj.kafka.producer.KafkaJsonSinker;
import io.numaproj.kafka.schema.Registry;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaApplication {

  private static final String ARG_CONFIG = "--config=";
  private static final String ARG_PREFIX = "--";

  private static final String KEY_HANDLER = "handler";
  private static final String KEY_PRODUCER_PROPERTIES_PATH = "producer.properties.path";
  private static final String KEY_CONSUMER_PROPERTIES_PATH = "consumer.properties.path";
  private static final String KEY_TOPIC_NAME = "topicName";
  private static final String KEY_SCHEMA_TYPE = "schemaType";
  private static final String KEY_SCHEMA_SUBJECT = "schemaSubject";
  private static final String KEY_SCHEMA_VERSION = "schemaVersion";

  private static final String HANDLER_CONSUMER = "consumer";
  private static final String HANDLER_PRODUCER = "producer";
  private static final String SCHEMA_TYPE_AVRO = "avro";
  private static final String SCHEMA_TYPE_JSON = "json";

  public static void main(String[] args) throws Exception {
    // TODO - validate the arguments, cannot enable both consumer and producer
    log.info("Supplied arguments: {}", (Object) args);
    Map<String, String> argMap = parseArgs(args);

    String handler = argMap.get(KEY_HANDLER);
    if (handler == null || handler.isBlank()) {
      // Infer handler from which properties arg is present for backward compatibility
      if (argMap.containsKey(KEY_PRODUCER_PROPERTIES_PATH)) {
        handler = HANDLER_PRODUCER;
      } else if (argMap.containsKey(KEY_CONSUMER_PROPERTIES_PATH)) {
        handler = HANDLER_CONSUMER;
      } else {
        throw new IllegalArgumentException(
            "--handler=[consumer|producer] is required, or pass --producer.properties.path / --consumer.properties.path");
      }
      log.info("Handler inferred as '{}' from properties path argument", handler);
    }

    UserConfig userConfig = buildUserConfig(argMap);
    log.info("UserConfig: {}", userConfig);

    switch (handler.toLowerCase()) {
      case HANDLER_CONSUMER -> startConsumer(argMap, userConfig);
      case HANDLER_PRODUCER -> startProducer(argMap, userConfig);
      default ->
          throw new IllegalArgumentException(
              "Unknown handler: " + handler + ". Must be 'consumer' or 'producer'");
    }
  }

  private static void startConsumer(Map<String, String> argMap, UserConfig userConfig)
      throws Exception {
    String consumerPropertiesPath = argMap.get(KEY_CONSUMER_PROPERTIES_PATH);
    if (consumerPropertiesPath == null) {
      throw new IllegalArgumentException(
          "--consumer.properties.path is required for consumer mode");
    }

    ConsumerConfig consumerConfig = new ConsumerConfig(consumerPropertiesPath);
    String groupId = consumerConfig.consumerGroupId();
    var adminClient = consumerConfig.kafkaAdminClient();
    Admin admin = new Admin(userConfig, groupId, adminClient);

    String schemaType = userConfig.getSchemaType();
    if (SCHEMA_TYPE_AVRO.equals(schemaType)) {
      var kafkaConsumer = consumerConfig.kafkaAvroConsumer();
      AvroWorker worker = new AvroWorker(userConfig, kafkaConsumer);
      AvroSourcer sourcer = new AvroSourcer(worker, admin);
      sourcer.startConsumer();
    } else {
      // json or raw
      var kafkaConsumer = consumerConfig.kafkaByteArrayConsumer();
      ByteArrayWorker worker = new ByteArrayWorker(userConfig, kafkaConsumer);
      ByteArraySourcer sourcer = new ByteArraySourcer(worker, admin);
      sourcer.startConsumer();
    }
  }

  private static void startProducer(Map<String, String> argMap, UserConfig userConfig)
      throws Exception {
    String producerPropertiesPath = argMap.get(KEY_PRODUCER_PROPERTIES_PATH);
    if (producerPropertiesPath == null) {
      throw new IllegalArgumentException(
          "--producer.properties.path is required for producer mode");
    }

    ProducerConfig producerConfig = new ProducerConfig(producerPropertiesPath);
    String schemaType = userConfig.getSchemaType();

    if (SCHEMA_TYPE_AVRO.equals(schemaType)) {
      var kafkaProducer = producerConfig.kafkaAvroProducer();
      var schemaRegistryClient = producerConfig.schemaRegistryClient();
      Registry registry = producerConfig.schemaRegistry(schemaRegistryClient);
      new KafkaAvroSinker(userConfig, kafkaProducer, registry).startSinker();
    } else if (SCHEMA_TYPE_JSON.equals(schemaType)) {
      var kafkaProducer = producerConfig.kafkaByteArrayProducer();
      var schemaRegistryClient = producerConfig.schemaRegistryClient();
      Registry registry = producerConfig.schemaRegistry(schemaRegistryClient);
      new KafkaJsonSinker(userConfig, kafkaProducer, registry).startSinker();
    } else {
      // raw
      var kafkaProducer = producerConfig.kafkaByteArrayProducer();
      new KafkaByteArraySinker(userConfig, kafkaProducer).startSinker();
    }
  }

  private static UserConfig buildUserConfig(Map<String, String> argMap) {
    String topicName = argMap.get(KEY_TOPIC_NAME);
    if (topicName == null || topicName.isBlank()) {
      throw new IllegalArgumentException("--topicName is required");
    }
    String schemaType = argMap.get(KEY_SCHEMA_TYPE);
    if (schemaType == null || schemaType.isBlank()) {
      throw new IllegalArgumentException(
          "--schemaType is required (avro, json, or raw)");
    }
    String schemaSubject = argMap.getOrDefault(KEY_SCHEMA_SUBJECT, "");
    int schemaVersion = Integer.parseInt(argMap.getOrDefault(KEY_SCHEMA_VERSION, "0"));
    return UserConfig.builder()
        .topicName(topicName)
        .schemaType(schemaType)
        .schemaSubject(schemaSubject)
        .schemaVersion(schemaVersion)
        .build();
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> map = new HashMap<>();
    String configPath = null;

    for (String arg : args) {
      if (arg.startsWith(ARG_CONFIG)) {
        configPath = arg.substring(ARG_CONFIG.length());
      } else if (arg.startsWith(ARG_PREFIX)) {
        int eq = arg.indexOf('=');
        if (eq > 0) {
          map.put(arg.substring(ARG_PREFIX.length(), eq), arg.substring(eq + 1));
        }
      }
    }

    // Fill in values from config file only where CLI args did not already provide them
    if (configPath != null) {
      loadConfigFile(configPath).forEach(map::putIfAbsent);
    }
    return map;
  }

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  private static Map<String, String> loadConfigFile(String path) {
    try {
      Map<String, Object> yaml = YAML_MAPPER.readValue(
          new File(path), new TypeReference<Map<String, Object>>() {});
      Map<String, String> result = new HashMap<>();
      yaml.forEach((k, v) -> {
        if (v != null) result.put(k, v.toString());
      });
      return result;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load config file: " + path, e);
    }
  }
}
