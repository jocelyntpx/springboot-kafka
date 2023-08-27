package main.java.com.springbootkafka.springbootkafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

//import com.riferrei.kafka.core.BucketPriorityAssignor;
//import com.riferrei.kafka.core.BucketPriorityConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import static io.confluent.kafka.demo.utils.KafkaUtils.createTopic;
import static io.confluent.kafka.demo.utils.KafkaUtils.getConfigs;

public class BucketConsumer {

    private class ConsumerThread extends Thread {

        private String threadName;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(String bucketName,
                              String threadName, Properties configs) {

            this.threadName = threadName;

            configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ORDERS_PER_BUCKET + "-group");

            // implementing the bucket priority pattern

            configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    BucketPriorityAssignor.class.getName());

            configs.put(BucketPriorityConfig.TOPIC_CONFIG, ORDERS_PER_BUCKET);
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
            configs.put(BucketPriorityConfig.BUCKET_CONFIG, bucketName);

            consumer = new KafkaConsumer<>(configs);
            consumer.subscribe(Arrays.asList(ORDERS_PER_BUCKET));

        }

        @Override
        public void run() {
            for (;;) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(Integer.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("[%s] Key = %s, Partition = %d",
                            threadName, record.key(), record.partition()));
                }
            }
        }

    }

    private final List<ConsumerThread> consumerThreads = new ArrayList<>();

    private void run(String bucketName, int numberOfThreads, Properties configs) {
        for (int i = 0; i < numberOfThreads; i++) {
            String threadName = String.format("%s-Thread-%d", bucketName, i);
            consumerThreads.add(new ConsumerThread(bucketName, threadName, configs));
        }
        consumerThreads.stream().forEach(ct -> ct.start());
    }

    private static final String ORDERS_PER_BUCKET = "orders-per-bucket";

    public static void main(String[] args) {
        createTopic(ORDERS_PER_BUCKET, 6, (short) 3);
        if (args.length >= 2) {
            String bucketName = args[0];
            int numberOfThreads = Integer.parseInt(args[1]);
            new BucketBasedConsumer().run(bucketName,
                    numberOfThreads, getConfigs());
        }
    }

}