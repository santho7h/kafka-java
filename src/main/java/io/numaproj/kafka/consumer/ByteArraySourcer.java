package io.numaproj.kafka.consumer;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.kafka.common.CommonUtils;
import io.numaproj.kafka.config.ConsumerConfig;
import io.numaproj.kafka.config.UserConfig;
import io.numaproj.numaflow.sourcer.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/**
 * ByteArraySourcer is the implementation of the Numaflow Sourcer to read raw messages in byte array
 * format from Kafka
 */
@Slf4j
public class ByteArraySourcer extends Sourcer {
  private final ConsumerConfig consumerConfig;
  private final UserConfig userConfig;
  private final Admin admin;

  // Worker and its thread are lazily initialized on the first read() call so that
  // we can configure max.poll.records from the Numaflow-supplied batch size.
  // Numaflow does not expose batch size and read timeout before the first ReadRequest arrives.
  private ByteArrayWorker worker;
  private Thread workerThread;

  // readTopicPartitionOffsetMap is used to keep track of the highest offsets read from the current
  // batch. The key is the topic:partition and the value is the highest offset read from the
  // partition. This map is used to validate the ack request, ensuring the ack request matches the
  // previous read request.
  private Map<String, Long> readTopicPartitionOffsetMap;

  public ByteArraySourcer(ConsumerConfig consumerConfig, UserConfig userConfig, Admin admin) {
    this.consumerConfig = consumerConfig;
    this.userConfig = userConfig;
    this.admin = admin;
  }

  /**
   * Lazily initializes the consumer worker using the batch size and timeout from the first
   * ReadRequest so that max.poll.records is aligned with the Numaflow pipeline configuration.
   */
  private synchronized void initWorkerIfNeeded(ReadRequest request) throws IOException {
    if (worker != null) {
      return;
    }
    int batchSize = (int) request.getCount();
    log.info(
        "Initializing byte array consumer worker with batchSize={} timeoutMs={}",
        batchSize,
        request.getTimeout().toMillis());
    var kafkaConsumer = consumerConfig.kafkaByteArrayConsumer(batchSize);
    worker = new ByteArrayWorker(userConfig, kafkaConsumer);
    workerThread = new Thread(worker, "consumerWorkerThread");
    workerThread.start();
  }

  public void startConsumer() throws Exception {
    log.info("Initializing Kafka byte array sourcer server...");
    new Server(this).start();
  }

  /**
   * This method is added mainly to assist with unit tests
   *
   * @return boolean if the consumer worker thread is alive
   */
  boolean isWorkerThreadAlive() {
    return workerThread != null && workerThread.isAlive();
  }

  /** Used in tests */
  @VisibleForTesting
  void setReadTopicPartitionOffsetMap(Map<String, Long> readTopicPartitionOffsetMap) {
    this.readTopicPartitionOffsetMap = readTopicPartitionOffsetMap;
  }

  /** Used in tests to inject a pre-built worker directly, bypassing lazy init */
  @VisibleForTesting
  void setWorker(ByteArrayWorker worker, Thread thread) {
    this.worker = worker;
    this.workerThread = thread;
  }

  public void kill(Exception e) {
    log.error("Received kill signal, shutting down the sourcer", e);
    System.exit(100);
  }

  @Override
  public void read(ReadRequest request, OutputObserver observer) {
    try {
      initWorkerIfNeeded(request);
    } catch (IOException e) {
      kill(new RuntimeException("Failed to initialize consumer worker", e));
      return;
    }

    // check if the consumer worker thread is still alive
    if (!isWorkerThreadAlive()) {
      log.error("Consumer worker thread is not alive, exiting...");
      kill(new RuntimeException("Consumer worker thread is not alive"));
    }

    int j = 0;
    readTopicPartitionOffsetMap = new HashMap<>();
    List<ConsumerRecord<String, byte[]>> consumerRecordList;
    try {
      consumerRecordList = worker.poll(request.getTimeout().toMillis());
    } catch (InterruptedException e) {
      kill(new RuntimeException(e));
      return;
    }

    if (consumerRecordList != null) {
      for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecordList) {
        if (consumerRecord == null) {
          continue;
        }
        Map<String, String> kafkaHeaders = new HashMap<>();
        for (Header header : consumerRecord.headers()) {
          kafkaHeaders.put(header.key(), new String(header.value()));
        }
        // TODO - Do we need to add cluster ID to the offset value?
        // For now, it's probably good enough.
        String offsetValue = consumerRecord.topic() + ":" + consumerRecord.offset();
        Message message =
            new Message(
                consumerRecord.value(),
                new Offset(
                    offsetValue.getBytes(StandardCharsets.UTF_8), consumerRecord.partition()),
                Instant.ofEpochMilli(consumerRecord.timestamp()),
                kafkaHeaders);
        observer.send(message);

        String key =
            CommonUtils.getTopicPartitionKey(consumerRecord.topic(), consumerRecord.partition());
        if (readTopicPartitionOffsetMap.containsKey(key)) {
          if (readTopicPartitionOffsetMap.get(key) < consumerRecord.offset()) {
            readTopicPartitionOffsetMap.put(key, consumerRecord.offset());
          }
        } else {
          readTopicPartitionOffsetMap.put(key, consumerRecord.offset());
        }
        j++;
      }
    }
    log.debug(
        "BatchRead summary: requested number of messages: {} number of messages sent: {} number of partitions: {} readTopicPartitionOffsetMap:{}",
        request.getCount(),
        j,
        readTopicPartitionOffsetMap.size(),
        readTopicPartitionOffsetMap);
  }

  @Override
  public void ack(AckRequest request) {
    Map<String, Long> topicPartitionOffsetMap = getPartitionToHighestOffsetMap(request);
    for (Map.Entry<String, Long> entry : topicPartitionOffsetMap.entrySet()) {
      if (readTopicPartitionOffsetMap == null
          || !readTopicPartitionOffsetMap.containsKey(entry.getKey())) {
        // TODO - emit error metrics
        log.error(
            "PANIC! THIS SHOULD NEVER HAPPEN. READ OFFSET MAP DOES NOT CONTAIN THE PARTITION ENTRY topic:partition:{}",
            entry.getKey());
      } else if (readTopicPartitionOffsetMap.get(entry.getKey()).longValue()
          != entry.getValue().longValue()) {
        // TODO - emit error metrics
        log.error(
            "PANIC! THIS SHOULD NEVER HAPPEN. READ AND ACK ARE NOT IN SYNC numa_ack:{} numa_read:{} topic:partition:{}",
            entry.getValue(),
            readTopicPartitionOffsetMap.get(entry.getKey()),
            entry.getKey());
      }
    }

    log.debug(
        "No. of offsets in AckRequest:{}  topicPartitionOffsetList:{}",
        request.getOffsets().size(),
        topicPartitionOffsetMap);
    try {
      worker.commit();
    } catch (InterruptedException e) {
      // Exit from the loop if the thread is interrupted
      kill(new RuntimeException(e));
    }
  }

  // This method is used to get the highest offset for each topic:partition from the AckRequest
  private static Map<String, Long> getPartitionToHighestOffsetMap(AckRequest request) {
    Map<String, Long> topicPartitionOffsetMap = new HashMap<>();
    for (Offset offset : request.getOffsets()) {
      String[] topicOffset = new String(offset.getValue(), StandardCharsets.UTF_8).split(":");
      String topicName = topicOffset[0];
      long tmpOffset = Long.parseLong(topicOffset[1]);
      String key = CommonUtils.getTopicPartitionKey(topicName, offset.getPartitionId());
      if (topicPartitionOffsetMap.containsKey(key)) {
        if (topicPartitionOffsetMap.get(key) < tmpOffset) {
          topicPartitionOffsetMap.put(key, tmpOffset);
        }
      } else {
        topicPartitionOffsetMap.put(key, tmpOffset);
      }
    }
    return topicPartitionOffsetMap;
  }

  @Override
  public long getPending() {
    return admin.getPendingMessages();
  }

  @Override
  public List<Integer> getPartitions() {
    if (worker == null) {
      return List.of();
    }
    return worker.getPartitions();
  }
}
