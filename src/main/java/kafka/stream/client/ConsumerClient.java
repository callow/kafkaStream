package kafka.stream.client;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import kafka.stream.common.KafkaHelper;

public class ConsumerClient {

    public static void main(String[] args) throws InterruptedException {
        Properties conf = new Properties();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.88.130:9092");
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "event.time.test");
        conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
        consumer.subscribe(Collections.singleton(KafkaHelper.TIME_TOPIC));
        final AtomicBoolean flag = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            flag.set(false);
        }));
        while (flag.get()) {
        	// 每分钟poll一次
            consumer.poll(Duration.ofMinutes(1)).forEach(record -> {
            	KafkaHelper.info("******** record timestamp:" + record.timestamp());
            	KafkaHelper.info("record key:" + record.key() + "value: " + record.value());
            });
        }

        consumer.close();
        KafkaHelper.info("The kafka consumer client is closed...");
    }
}
