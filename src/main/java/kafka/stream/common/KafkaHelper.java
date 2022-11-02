package kafka.stream.common;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

public class KafkaHelper {
	
	 public final static String BOOTSTRAP_SERVER = "localhost:9092";
	 
	 public static final String FIRST_APP_ID = "first_streams_app_id";
	 public static final String FIRST_APP_SOURCE_TOPIC = "input.words";
	 public static final String FIRST_APP_TARGET_TOPIC = "output.words";
	 
	 public final static String XMALL_TRANSACTION_SOURCE_TOPIC = "xmall.transaction";
	 public final static String XMALL_TRANSACTION_PATTERN_TOPIC = "xmall.pattern.transaction";
	 public final static String XMALL_TRANSACTION_REWARDS_TOPIC = "xmall.rewards.transaction";
	 public final static String XMALL_TRANSACTION_PURCHASES_TOPIC = "xmall.purchases.transaction";
	 public final static String XMALL_TRANSACTION_COFFEE_TOPIC = "xmall.coffee.transaction";
	 public final static String XMALL_TRANSACTION_ELECT_TOPIC = "xmall.elect.transaction";
	 
	 public final static String STATELESS_MAP_APP_ID = "stateless_map_operation";
	 public final static String  STATELESS_MAPVALUE_APP_ID = "stateless_mapValues_operation";
	 public final static String STATELESS_FILTER_APP_ID = "stateless_Filtering_operation";
	 public final static String STATELESS_FLAT_MAP_APP_ID = "stateless_flatMap_operation";
	 public final static String STATELESS_FLAT_MAPVALUE_APP_ID = "stateless_flatMapValues_operation";
	 public final static String STATELESS_SELECT_KEY_APP_ID = "stateless_selectKey_operation";
	 public final static String STATELESS_FOREACH_APP_ID = "stateless_foreach_operation";
	 public final static String STATELESS_SPLIT_MERGE_APP_ID = "stateless_split_merge_operation";
	 public final static String STATELESS_SINK_APP_ID = "stateless_sink_operation";
	 public final static String STATELESS_XMALL_APP_ID = "xmall_app";

	 
	 
	 public static void info(String msg) {
		 System.out.println(msg);
	 }
	 
	 public static Properties config(String appId) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        // 指定默认序列化的泛型
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
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
