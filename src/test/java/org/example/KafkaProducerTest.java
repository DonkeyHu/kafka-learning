package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
/**
 * kafka producer test
 */
public class KafkaProducerTest
{

    @Test
    public void produce() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.ACKS_CONFIG,"1");

        KafkaProducer<String,String> producer = new KafkaProducer(properties);

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 5; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("ooxx-item", "item" + j, "val" + i);
                RecordMetadata metadata = producer.send(record).get();
                int partition = metadata.partition();
                long offset = metadata.offset();
                System.out.println("key: "+record.key()+" value: "+record.value()+" partition: "+partition+" offset: "+offset);
            }
        }
    }

}
