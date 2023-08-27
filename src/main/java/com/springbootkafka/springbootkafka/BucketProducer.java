package main.java.com.springbootkafka.springbootkafka;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

//import com.riferrei.kafka.core.BucketPriorityConfig;
//import com.riferrei.kafka.core.BucketPriorityPartitioner;
//import com.riferrei.kafka.core.DiscardPartitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import static io.confluent.kafka.demo.utils.KafkaUtils.createTopic;
import static io.confluent.kafka.demo.utils.KafkaUtils.getConfigs;

public class BucketProducer {

    private void run(Properties configs) {

        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());


        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                BucketPriorityPartitioner.class.getName());

        configs.put(BucketPriorityConfig.TOPIC_CONFIG, ORDERS_PER_BUCKET);
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 30%");
        configs.put(BucketPriorityConfig.FALLBACK_PARTITIONER_CONFIG,
                DiscardPartitioner.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {

            AtomicInteger counter = new AtomicInteger(0);
            String[] buckets = {"Platinum", "Gold"};

            for (;;) {

                int value = counter.incrementAndGet();
                final String recordKey = "Platinum-" + value;

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(ORDERS_PER_BUCKET, recordKey, "Value");

                producer.send(record, (metadata, exception) -> {
                    System.out.println(String.format(
                            "Key '%s' was sent to partition %d",
                            recordKey, metadata.partition()));
                });

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }

            }

        }

    }

    private static final String ORDERS_PER_BUCKET = "orders-per-bucket";

    public static void main(String[] args) {
        createTopic(ORDERS_PER_BUCKET, 6, (short) 3);
        new BucketBasedProducer().run(getConfigs());
    }

}