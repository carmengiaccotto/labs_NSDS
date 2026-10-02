package it.polimi.middleware.kafka.Eval25;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;


// Group members: Callegari Alessandro, Franchetti-Rosada Alessia, Giaccotto Carmen

// Is it possible to have more than one partition for topics "sensors1" and "sensors2"?
//Yes
// Is there any relation between the number of partitions in "sensors1" and "sensors2"?
// Yes, "sensor1" and "sensor2" should have the same number of partitions.

// Is it possible to have more than one instance of Merger?
//Yes
// If so, what is the relation between their group id?
// All Merger's instances should have the same group id

// Is it possible to have more than one partition for topic "merged"?
//Yes

// Is it possible to have more than one instance of Validator?
// Yes, the number of Validator's instances that can process messages in parallel is limited by the number of partitions in the merged topic.
// If so, what is the relation between their group id?
// All Validator's instances should have the same group id.

public class Consumers {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        String transactionId = args[2];
        switch (stage) {
            case 0:
                new Merger(serverAddr, groupId).execute();
                break;
            case 1:
                new Validator(serverAddr, groupId, transactionId).execute();
                break;
            case 2:
                System.err.println("Wrong stage");
        }
    }

    private static class Merger {
        private final String serverAddr;
        private final String consumerGroupId;
        private static final String sensor1Topic = "sensors1";
        private static final String sensor2Topic = "sensors2";
        private static final String MergedTopic = "MergedTopic";

        final Map<String, Integer> lastValueKeySensor1 = new HashMap<>();
        final Map<String, Integer> lastValueKeySensor2 = new HashMap<>();

        public Merger(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Arrays.asList(sensor1Topic, sensor2Topic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));


            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                for (final ConsumerRecord<String, Integer> record : records) {
                    System.out.println("Partition: " + record.partition() +
                            "\tOffset: " + record.offset() +
                            "\tKey: " + record.key() +
                            "\tValue: " + record.value()
                    );
                    int sum = 0;
                    if (record.topic().equals(sensor1Topic)) {
                        lastValueKeySensor1.put(record.key(), record.value());
                        if (lastValueKeySensor2.containsKey(record.key())) {
                            sum = lastValueKeySensor1.get(record.key()) + lastValueKeySensor2.get(record.key());
                        } else {
                            sum = lastValueKeySensor1.get(record.key());
                        }
                    } else if (record.topic().equals(sensor2Topic)) {
                        lastValueKeySensor2.put(record.key(), record.value());
                        if (lastValueKeySensor1.containsKey(record.key())) {
                            sum = lastValueKeySensor1.get(record.key()) + lastValueKeySensor2.get(record.key());
                        } else {
                            sum = lastValueKeySensor2.get(record.key());
                        }
                    }
                    producer.send(new ProducerRecord<>(MergedTopic, record.key(), sum));
                    offsetsToCommit.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                }
                producer.flush();
                consumer.commitSync(offsetsToCommit);
            }
        }
    }

    private static class Validator {
        private final String serverAddr;
        private final String consumerGroupId;
        private final String producerTransactionalId;
        private static final String MergedTopic = "MergedTopic";
        private static final String output1Topic = "output1";
        private static final String output2Topic = "output2";


        public Validator(String serverAddr, String consumerGroupId, String transactionalId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
            this.producerTransactionalId = transactionalId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(MergedTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.producerTransactionalId );
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

            final KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                producer.beginTransaction();
                try {
                    for (final ConsumerRecord<String, Integer> record : records) {
                        System.out.println("Partition: " + record.partition() +
                                "\tOffset: " + record.offset() +
                                "\tKey: " + record.key() +
                                "\tValue: " + record.value()
                        );
                        producer.send(new ProducerRecord<>(output1Topic, record.key(), record.value()));
                        producer.send(new ProducerRecord<>(output2Topic, record.key(), record.value()));
                    }

                    final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    for (final TopicPartition partition : records.partitions()) {
                        final List<ConsumerRecord<String, Integer>> partitionRecords = records.records(partition);
                        final long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        map.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }

                    producer.sendOffsetsToTransaction(map, consumer.groupMetadata());
                    producer.commitTransaction();
                } catch (KafkaException e) {
                    producer.abortTransaction();
                    System.err.println("Transaction aborted (will retry on next poll): " + e.getMessage());
                }
            }
        }
    }
}