package main.java.com.springbootkafka.springbootkafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.utils.Utils;

public class BucketPriorityPartitioner implements Partitioner {

    private BucketPriorityConfig config;
    private ThreadLocal<String> lastBucket;
    private Map<String, Bucket> buckets;
    private int lastPartitionCount;

    @Override
    public void configure(Map<String, ?> configs) {
        config = new BucketPriorityConfig(configs);
        List<Integer> allocation = new ArrayList<>(config.allocation().size());
        for (String allocItem : config.allocation()) {
            allocItem = allocItem.replaceAll("%", "").trim();
            allocation.add(Integer.parseInt(allocItem));
        }
        if (config.buckets().size() != allocation.size()) {
            throw new InvalidConfigurationException("The bucket allocation " +
                    "doesn't match with the number of buckets configured.");
        }
        int sumAllBuckets = allocation.stream().mapToInt(Integer::intValue).sum();
        if (sumAllBuckets != 100) {
            throw new InvalidConfigurationException("The bucket allocation " +
                    "is incorrect. The sum of all buckets needs to be 100.");
        }
        lastBucket = new ThreadLocal<>();
        buckets = new LinkedHashMap<>();
        for (int i = 0; i < config.buckets().size(); i++) {
            String bucketName = config.buckets().get(i).trim();
            buckets.put(bucketName, new Bucket(allocation.get(i)));
        }
        // sort the buckets from higher to lower allocation
        // aid later during allocation if unassigned partitions are found
        buckets = buckets.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        int partition = RecordMetadata.UNKNOWN_PARTITION;
        // try applying bucket priority partitioning logic
        // if none of the conditions apply, allow the partition to be
        // set by the built-in partitioning logic from KIP-794
        if (config.topic() != null && config.topic().equals(topic)) {
            if (key instanceof String) {
                String keyValue = (String) key;
                String[] keyValueParts = keyValue.split(config.delimiter());
                if (keyValueParts.length >= 1) {
                    String bucketName = keyValueParts[0].trim();
                    if (buckets.containsKey(bucketName)) {
                        lastBucket.set(bucketName);
                        partition = getPartition(bucketName, cluster);
                    }
                }
            }
        }
        return partition;
    }

    private int getPartition(String bucketName, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(config.topic());
        // check if number of partitions has changed,
        // trigger update if yes
        if (lastPartitionCount != numPartitions) {
            updatePartitionsAssignment(cluster);
            lastPartitionCount = numPartitions;
        }
        Bucket bucket = buckets.get(bucketName);
        return bucket.nextPartition();
    }

    private void updatePartitionsAssignment(Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(config.topic());
        if (partitions.size() < buckets.size()) {
            StringBuilder message = new StringBuilder();
            message.append("The number of partitions available for the topic '");
            message.append(config.topic()).append("' is incompatible with the ");
            message.append("number of buckets. It needs to be at least ");
            message.append(buckets.size()).append(".");
            throw new InvalidConfigurationException(message.toString());
        }
        // sort partitions in ascending order since the partitions will
        // be mapped into the buckets from partition-0 to partition-n
        partitions = partitions.stream()
                .sorted(Comparator.comparing(PartitionInfo::partition))
                .collect(Collectors.toList());
        // design the layout of the distribution
        int distribution = 0;
        Map<String, Integer> layout = new LinkedHashMap<>();
        for (Map.Entry<String, Bucket> entry : buckets.entrySet()) {
            int bucketSize = entry.getValue().size(partitions.size());
            layout.put(entry.getKey(), bucketSize);
            distribution += bucketSize;
        }
        // check if unassigned partitions exists,
        // if exists, distribute them over the buckets starting from the top to bottom
        // until there are no partitions left
        int remaining = partitions.size() - distribution;
        if (remaining > 0) {
            AtomicInteger counter = new AtomicInteger(-1);
            List<String> availableBuckets = new ArrayList<>();
            buckets.keySet().stream().forEach(bucket -> {
                availableBuckets.add(bucket);
            });
            while (remaining > 0) {
                int nextValue = counter.incrementAndGet();
                int index = Utils.toPositive(nextValue) % availableBuckets.size();
                String bucketName = availableBuckets.get(index);
                int bucketSize = layout.get(bucketName);
                layout.put(bucketName, ++bucketSize);
                remaining--;
            }
        }
        // lastly, assign the available partitions to buckets
        int partition = -1;
        TopicPartition topicPartition = null;
        bucketAssign: for (Map.Entry<String, Bucket> entry : buckets.entrySet()) {
            int bucketSize = layout.get(entry.getKey());
            entry.getValue().getPartitions().clear();
            for (int i = 0; i < bucketSize; i++) {
                topicPartition = new TopicPartition(config.topic(), ++partition);
                entry.getValue().getPartitions().add(topicPartition);
                if (partition == partitions.size() - 1) {
                    break bucketAssign;
                }
            }
        }
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        // function to stabilize the round-robin logic.
        String bucketName = lastBucket.get();
        Bucket bucket = buckets.get(bucketName);
        if (bucket != null) {
            bucket.decrementCounter();
        }
        lastBucket.remove();
    }

    @Override
    public void close() {

    }

}