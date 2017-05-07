import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by afsalthaj on 7/05/2017.
 */
public class KafkaProducerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // Kafka bootstrap server
        // producer acks
        // topic names
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("acks","1");
        properties.setProperty("retries","3");
        properties.setProperty("linger.ms", "1");

        Producer<String, String> producer =  new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> producerRecord;
        for (int i = 10; i <=20 ; i ++) {
            producerRecord =
                    new ProducerRecord<String, String>("second_topic", Integer.toString(i), "the message with key: "+ i);
            producer.send(producerRecord);

        }

        producer.close();
    }
}
