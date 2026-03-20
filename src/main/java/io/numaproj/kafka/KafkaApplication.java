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

  public static void main(String[] args) throws Exception {
    // TODO - validate the arguments, cannot enable both consumer and producer
    log.info("Supplied arguments: {}", (Object) args);
    Map<String, String> argMap = parseArgs(args);

    String handler = argMap.get("handler");
    if (handler == null || handler.isBlank()) {
      throw new IllegalArgumentException(
          "--handler=[consumer|producer] is required");
    }

    UserConfig userConfig = buildUserConfig(argMap);
    log.info("UserConfig: {}", userConfig);

    switch (handler.toLowerCase()) {
      case "consumer" -> startConsumer(argMap, userConfig);
      case "producer" -> startProducer(argMap, userConfig);
      default ->
          throw new IllegalArgumentException(
              "Unknown handler: " + handler + ". Must be 'consumer' or 'producer'");
    }
  }

  private static void startConsumer(Map<String, String> argMap, UserConfig userConfig)
      throws Exception {
    String consumerPropertiesPath = argMap.get("consumer.properties.path");
    if (consumerPropertiesPath == null) {
      throw new IllegalArgumentException(
          "--consumer.properties.path is required for consumer mode");
    }

    ConsumerConfig consumerConfig = new ConsumerConfig(consumerPropertiesPath);
    String groupId = consumerConfig.consumerGroupId();
    var adminClient = consumerConfig.kafkaAdminClient();
    Admin admin = new Admin(userConfig, groupId, adminClient);

    String schemaType = userConfig.getSchemaType();
    if ("avro".equals(schemaType)) {
      var kafkaConsumer = consumerConfig.kafkaAvroConsumer();
      AvroWorker worker = new AvroWorker(userConfig, kafkaConsumer);
      AvroSourcer sourcer = new AvroSourcer(worker, admin);
      addConsumerShutdownHook(worker::shutdown, admin);
      sourcer.startConsumer();
    } else {
      // json or raw
      var kafkaConsumer = consumerConfig.kafkaByteArrayConsumer();
      ByteArrayWorker worker = new ByteArrayWorker(userConfig, kafkaConsumer);
      ByteArraySourcer sourcer = new ByteArraySourcer(worker, admin);
      addConsumerShutdownHook(worker::shutdown, admin);
      sourcer.startConsumer();
    }
  }

  private static void startProducer(Map<String, String> argMap, UserConfig userConfig)
      throws Exception {
    String producerPropertiesPath = argMap.get("producer.properties.path");
    if (producerPropertiesPath == null) {
      throw new IllegalArgumentException(
          "--producer.properties.path is required for producer mode");
    }

    ProducerConfig producerConfig = new ProducerConfig(producerPropertiesPath);
    String schemaType = userConfig.getSchemaType();

    if ("avro".equals(schemaType)) {
      var kafkaProducer = producerConfig.kafkaAvroProducer();
      var schemaRegistryClient = producerConfig.schemaRegistryClient();
      Registry registry = producerConfig.schemaRegistry(schemaRegistryClient);
      KafkaAvroSinker sinker = new KafkaAvroSinker(userConfig, kafkaProducer, registry);
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      sinker.close();
                    } catch (Exception e) {
                      log.error("Error closing avro sinker during shutdown", e);
                    }
                  }));
      sinker.startSinker();
    } else if ("json".equals(schemaType)) {
      var kafkaProducer = producerConfig.kafkaByteArrayProducer();
      var schemaRegistryClient = producerConfig.schemaRegistryClient();
      Registry registry = producerConfig.schemaRegistry(schemaRegistryClient);
      KafkaJsonSinker sinker = new KafkaJsonSinker(userConfig, kafkaProducer, registry);
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      sinker.close();
                    } catch (Exception e) {
                      log.error("Error closing json sinker during shutdown", e);
                    }
                  }));
      sinker.startSinker();
    } else {
      // raw
      var kafkaProducer = producerConfig.kafkaByteArrayProducer();
      KafkaByteArraySinker sinker = new KafkaByteArraySinker(userConfig, kafkaProducer);
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> sinker.close()));
      sinker.startSinker();
    }
  }

  private static UserConfig buildUserConfig(Map<String, String> argMap) {
    String topicName = argMap.get("topicName");
    if (topicName == null || topicName.isBlank()) {
      throw new IllegalArgumentException("--topicName is required");
    }
    String schemaType = argMap.get("schemaType");
    if (schemaType == null || schemaType.isBlank()) {
      throw new IllegalArgumentException(
          "--schemaType is required (avro, json, or raw)");
    }
    String schemaSubject = argMap.getOrDefault("schemaSubject", "");
    int schemaVersion = Integer.parseInt(argMap.getOrDefault("schemaVersion", "0"));
    return UserConfig.builder()
        .topicName(topicName)
        .schemaType(schemaType)
        .schemaSubject(schemaSubject)
        .schemaVersion(schemaVersion)
        .build();
  }

  @FunctionalInterface
  private interface WorkerShutdown {
    void shutdown() throws InterruptedException;
  }

  private static void addConsumerShutdownHook(WorkerShutdown workerShutdown, Admin admin) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    workerShutdown.shutdown();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  admin.close();
                }));
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> map = new HashMap<>();
    String configPath = null;

    for (String arg : args) {
      if (arg.startsWith("--config=")) {
        configPath = arg.substring("--config=".length());
      } else if (arg.startsWith("--")) {
        int eq = arg.indexOf('=');
        if (eq > 0) {
          map.put(arg.substring(2, eq), arg.substring(eq + 1));
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
