package kafka.stream.common;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import kafka.stream.ktable.model.ShootStats;

public class KafkaHelper {
	
	 public final static String BOOTSTRAP_SERVER = "localhost:9092";
	 public final static String STATE_STORE_NAME = "stateful_transform_operation";
     public final static String SHOOT_STATE_STORE = "shoot_state_store";
	 
	 
	 public static final String FIRST_APP_SOURCE_TOPIC = "input.words";
	 public static final String FIRST_APP_TARGET_TOPIC = "output.words";
	 public static final String SALES_SOURCE_TOPIC = "sales";
	 public final static String TRAFFIC_LOG_SOURCE_TOPIC = "net.traffic.logs";
	 public final static String HEART_BEAT_TOPIC = "heartbeat";
	 public final static String PATIENT_TOPIC = "patient";
	 public final static String SICK_ROOM_TOPIC = "sickroom";
	 //<alex,wang>
	 public final static String USERS_SOURCE_TOPIC = "users";
	 
	 public final static String XMALL_TRANSACTION_SOURCE_TOPIC = "xmall.transaction";
	 public final static String XMALL_TRANSACTION_PATTERN_TOPIC = "xmall.pattern.transaction";
	 public final static String XMALL_TRANSACTION_REWARDS_TOPIC = "xmall.rewards.transaction";
	 public final static String XMALL_TRANSACTION_PURCHASES_TOPIC = "xmall.purchases.transaction";
	 public final static String XMALL_TRANSACTION_COFFEE_TOPIC = "xmall.coffee.transaction";
	 public final static String XMALL_TRANSACTION_ELECT_TOPIC = "xmall.elect.transaction";
   
     public final static String USER_INFO_TOPIC = "user.info";  //001,Alex
     public final static String USER_ADDRESS_TOPIC = "user.address";//001,CN
     
     public final static String EMP_SOURCE_TOPIC = "employee";
     public final static String EMP_TARGET_TOPIC = "employee_cloud_less_65_with_title";
     public final static String SHOOT_SOURCE_TOPIC = "shoot.game";

	 
     public static final String FIRST_APP_ID = "first_streams_app_id";
	 public final static String STATELESS_MAP_APP_ID = "stateless_map_operation";
	 public final static String STATELESS_MAPVALUE_APP_ID = "stateless_mapValues_operation";
	 public final static String STATELESS_FILTER_APP_ID = "stateless_Filtering_operation";
	 public final static String STATELESS_FLAT_MAP_APP_ID = "stateless_flatMap_operation";
	 public final static String STATELESS_FLAT_MAPVALUE_APP_ID = "stateless_flatMapValues_operation";
	 public final static String STATELESS_SELECT_KEY_APP_ID = "stateless_selectKey_operation";
	 public final static String STATELESS_FOREACH_APP_ID = "stateless_foreach_operation";
	 public final static String STATELESS_SPLIT_MERGE_APP_ID = "stateless_split_merge_operation";
	 public final static String STATELESS_SINK_APP_ID = "stateless_sink_operation";
	 public final static String STATELESS_XMALL_APP_ID = "xmall_app";
	 public final static String STATEFUL_XMALL_APP_ID = "xmall_app_2";
	 public final static String TIME_TOPIC = "time.append";
	 
	 public final static String STATEFUL_TRANSFORM_APP_ID = "stateful_transform_operation";
	 public final static String STATEFUL_INNER_JOIN_APP_ID = "stateful_inner_join_app";
	 public final static String STATEFUL_LEFT_JOIN_APP_ID = "stateful_left_join_app";
	 public final static String STATEFUL_FULL_JOIN_APP_ID = "stateful_left_out_join_app";
	 public final static String STATEFUL_COUNT_AGGREGATE_APP_ID = "stateful_count_aggregation_app";
	 public final static String STATEFUL_REDUCE_AGGREGATE_APP_ID = "stateful_reduce_aggregation_app";
	 public final static String STATEFUL_REDUCE_SALE_CHAMPION_APP_ID = "stateful_reduce_sales_champion_app";
	 public final static String STATEFUL_AGGREDATE_SALE_CHAMPION_APP_ID = "stateful_aggregate_sales_stats_app";
	 
	 public final static String WINDOW_TUMBLING_APP_ID = "window_time_tumbling";
	 public final static String WINDOW_SLIDING_APP_ID = "window_time_sliding";
	 public final static String WINDOW_HOPPING_APP_ID = "hopping_time_windowing";
	 public final static String WINDOW_SESSION_APP_ID = "session_time_window_app";
	 public final static String WINDOW_PATIENT_APP_ID = "patient_heart_beat_monitor_app";

	 public final static String KTABLE_DIRECT_APP_ID = "create_ktable_from_kstream";
	 public final static String KTABLE_KSTREAM_APP_ID = "create_ktable_from_builder";
	 public final static String KTABLE_BASIC_APP_ID = "ktable_basis_operation_app";
	 public final static String KTABLE_TRANSFORM_VALUES_APP_ID = "ktable_transform_values_app";
	 public final static String KTABLE_JOIN_APP_ID = "kstream_inner_join_ktable";

	 
	 
	 public static final StoreBuilder<KeyValueStore<String, Integer>> STRING_INT_STOREBUILDER = Stores.keyValueStoreBuilder(
		        Stores.persistentKeyValueStore(STATE_STORE_NAME), Serdes.String(), Serdes.Integer());

 
	 public static final StoreBuilder<KeyValueStore<String, ShootStats>> STRING_SHOOT_STOREBUILDER = Stores.keyValueStoreBuilder(
             Stores.persistentKeyValueStore(SHOOT_STATE_STORE), Serdes.String(), JsonSerdes.ShootStatsSerde()
     );
	 
	 public static void info(String msg) {
		 System.out.println(msg);
	 }
	 
	 public static StreamsBuilder streamBuilderwithoutStore() {
		return new StreamsBuilder();
	 }
	 
	 public static StreamsBuilder streamBuilderwithShootStore() {
			StreamsBuilder builder = new StreamsBuilder();
			builder.addStateStore(STRING_SHOOT_STOREBUILDER);
			return builder;
		 
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
        
        //TODO 取消Group的缓存 让其立即生效 => no recommendation
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        
        // windowing使用的是wall time = 系统服务器时间
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
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
