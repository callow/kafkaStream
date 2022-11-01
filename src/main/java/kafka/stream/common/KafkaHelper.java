package kafka.stream.common;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

public class KafkaHelper {
	
	 public final static String BOOTSTRAP_SERVER = "localhost:9092";
	 
	 public static final String FIRST_APP_ID = "first_streams_app_id";
	 public static final String FIRST_APP_SOURCE_TOPIC = "input.words";
	 public static final String FIRST_APP_TARGET_TOPIC = "output.words";
	 
	 public final static String STATELESS_APP_ID = "stateless_map_operation";
	 
	 
	 public static void info(String msg) {
		 System.out.println(msg);
	 }
	 
	 public static Properties config(String appId) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        return properties;
	 }
	 	 
	 public static void start(KafkaStreams kafkaStreams) {
	       CountDownLatch latch = new CountDownLatch(1);
	        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	        	//6. stop(graceful)
	            kafkaStreams.close();
	            latch.countDown();
	        }));

	        //5. start
	        kafkaStreams.start();
	        System.out.println("Kafka stream done!");
	        try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	 }
}
