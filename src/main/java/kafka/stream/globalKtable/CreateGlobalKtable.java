package kafka.stream.globalKtable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class CreateGlobalKtable {

	public static void main(String[] args) {
		
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();

		// 创建一个Stream
        KStream<String, String> userInfoKS = builder.stream(KafkaHelper.USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("user-source").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        // 创建一个GKT,
        GlobalKTable<String, String> globalKTable = builder.globalTable(KafkaHelper.USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("address-source")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST), Materialized.as("global-table"));

        // 开始Join
        userInfoKS.join(globalKTable,
        		(k, v) -> k, // 指定Key
        			(username, address) -> String.format("%s come from %s", username, address), 
        		 Named.as("join"))
                .print(Printed.<String, String>toSysOut().withLabel("join-result"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.GLOBAL_KTABLE_APP_ID)));
        
	}
}
