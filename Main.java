import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    public static void main(String args[]) {
        String log4jConfPath = "log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
        System.out.println("hola");
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","devs4j-group");
        props.setProperty("auto.offset.reset","earliest");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try(Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);) {
            consumer.subscribe(Arrays.asList("devs4j-topic"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Offset={}, Partition={}, Key={}, Value={}", consumerRecord.offset(),
                            consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
//                    consumer.commitSync();
                }
            }
        }
//        consume();
//        consumeAssignAndSeek();
    }

    public static void consume() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","devs4j-group");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("isolation.level","read_committed");
        props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try(Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);) {
            consumer.subscribe(Arrays.asList("devs4j-topic"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Offset={}, Partition={}, Key={}, Value={}", consumerRecord.offset(),
                            consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }

    public static void consumeAssignAndSeek() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","devs4j-group");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try(Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);) {
            TopicPartition topicPartition = new TopicPartition("devs4j-topic", 3);
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seek(topicPartition, 50);
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Offset={}, Partition={}, Key={}, Value={}", consumerRecord.offset(),
                            consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }
}
