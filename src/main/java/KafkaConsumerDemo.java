import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by afsalthaj on 7/05/2017.
 */
public class KafkaConsumerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // Kafka bootstrap server
        // producer acks
        // topic names
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id","mygroupid");
        properties.setProperty("auto.offset.reset","earliest");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.commit.interval.ms","1000");



        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("second_topic"));

        while(true) {
            System.out.println("entered");
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> cr : consumerRecords) {
                System.out.print(cr.key() + "\t");
                System.out.print(cr.value() + "\t");
                System.out.print(cr.partition() + "\t");
                System.out.print(cr.offset());
                System.out.println("*********************MESSAGE DETAILS FINISHED");
            }
        }
    }
}
