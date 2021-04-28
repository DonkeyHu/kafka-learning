package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

/**
 * consumer 提交offset的三种粒度：
 * 1.每消费一条数据，提交一次
 * 2.每消费完一个分区的数据提交一次
 * 3.每消费一个批次数据提交一次
 */
public class KafkaConsumerTest {

    @Test
    public void consume(){
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"gpdonkey001");
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
//        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"20000");
//        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(Arrays.asList("ooxx-item"), new ConsumerRebalanceListener() {
            // 消费组里面增加consumer了，那么会导致Partition重新分配
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("--onPartitionsRevoked--");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()){
                    System.out.println("Revoke partition "+iterator.next().partition());
                }
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("--onPartitionsAssigned--");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()){
                    System.out.println("Assigned partition "+iterator.next().partition());
                }
            }
        });

        while (true){
            // 微批，联想大数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0)); // 0意味着没有数据会持续等待，直到有数据poll下来
            while (!records.isEmpty()){
                System.out.println("consumer count is "+records.count());

                // 1.普通迭代数据
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()){
                    ConsumerRecord<String, String> record = iterator.next();
                    int partition = record.partition();
                    long offset = record.offset();
                    System.out.println("key: "+record.key()+" value: "+record.value()+" partition: "+partition+" offset: "+offset);
                }

                // 2.按分区partition去迭代数据
//                Set<TopicPartition> partitions = records.partitions();
//                for (TopicPartition partition : partitions) {
//                    List<ConsumerRecord<String, String>> pRecords = records.records(partition);
//                    for (ConsumerRecord<String, String> pRecord : pRecords) {
//                        int par = pRecord.partition();
//                        long offset = pRecord.offset();
//                        System.out.println("key: "+pRecord.key()+" value: "+pRecord.value()+" partition: "+par+" offset: "+offset);
//
//                        // a.每消费一条数据，提交一次offset
//                        OffsetAndMetadata om = new OffsetAndMetadata(offset);
//                        Map<TopicPartition,OffsetAndMetadata> map = new HashMap<>();
//                        map.put(partition,om);
//                        consumer.commitSync(map);
//                    }
//                    // b.每消费一个分区的数据，提交一次offset（这种情况最适合开辟多线程去处理，每个线程对应一个分区）
//                    OffsetAndMetadata om = new OffsetAndMetadata(pRecords.get(pRecords.size()-1).offset());
//                    Map<TopicPartition,OffsetAndMetadata> map = new HashMap<>();
//                    map.put(partition,om);
//                    consumer.commitSync(map);
//                }
//                // c. 每消费完整个poll批次的数据，提交一次offset
//                consumer.commitSync();
            }


        }
    }

}
