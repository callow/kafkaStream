package kafka.stream.client;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import kafka.stream.common.KafkaHelper;

public class ProducerClient {
    private final static String BOOTSTRAP_SERVER = "192.168.88.130:9092";
    private final static String TOPIC = "time.append";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties conf = new Properties();
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        conf.put(ProducerConfig.ACKS_CONFIG, "all");
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(conf);
        long currentTime = System.currentTimeMillis();
        KafkaHelper.info("\"*********** event time: " + currentTime);
        // 不指定partiton,随机
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, null, currentTime, null, "test data");

        Future<RecordMetadata> future = producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
            	KafkaHelper.info("The record is sent: " + metadata);
            }
        });
        producer.flush();
        future.get();
        producer.close();
    }
}
