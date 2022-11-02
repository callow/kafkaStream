package kafka.stream.common;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class KafkaHelper {
	
	 public final static String BOOTSTRAP_SERVER = "localhost:9092";
	 public final static String STATE_STORE_NAME = "stateful_transform_operation";
	 
	 public static final String FIRST_APP_ID = "first_streams_app_id";
	 public static final String FIRST_APP_SOURCE_TOPIC = "input.words";
	 public static final String FIRST_APP_TARGET_TOPIC = "output.words";
	 
	 public final static String XMALL_TRANSACTION_SOURCE_TOPIC = "xmall.transaction";
	 public final static String XMALL_TRANSACTION_PATTERN_TOPIC = "xmall.pattern.transaction";
	 public final static String XMALL_TRANSACTION_REWARDS_TOPIC = "xmall.rewards.transaction";
	 public final static String XMALL_TRANSACTION_PURCHASES_TOPIC = "xmall.purchases.transaction";
	 public final static String XMALL_TRANSACTION_COFFEE_TOPIC = "xmall.coffee.transaction";
	 public final static String XMALL_TRANSACTION_ELECT_TOPIC = "xmall.elect.transaction";
   
     public final static String USER_INFO_TOPIC = "user.info";  //001,Alex
     public final static String USER_ADDRESS_TOPIC = "user.address";//001,CN
	 
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
	 public final static String STATEFUL_XMALL_APP_ID = "xmall_app_2";
	 
	 public final static String STATEFUL_TRANSFORM_APP_ID = "stateful_transform_operation";
	 public final static String STATEFUL_INNER_JOIN_APP_ID = "stateful_inner_join_app";
	 
	 
	 public static final StoreBuilder<KeyValueStore<String, Integer>> STRING_INT_STOREBUILDER = Stores.keyValueStoreBuilder(
		        Stores.persistentKeyValueStore(STATE_STORE_NAME), Serdes.String(), Serdes.Integer());

	 
	 
	 public static void info(String msg) {
		 System.out.println(msg);
	 }
	 
	 public static StreamsBuilder streamBuilderwithoutStore() {
		return new StreamsBuilder();
	 }
	 
	 public static StreamsBuilder streamBuilderwithStore() {
		StreamsBuilder builder = new StreamsBuilder();
		builder.addStateStore(STRING_INT_STOREBUILDER);
		return builder;
	 }
	 
	 public static Properties config(String appId) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        // 指定默认序列化的泛型
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // 指定state store本地路径
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "D:\\kafka_2.12-3.2.1\\statestore");
        // 定义并发度，可以根据partiton数量决定
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
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
