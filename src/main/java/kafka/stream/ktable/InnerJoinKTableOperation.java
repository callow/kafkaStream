package kafka.stream.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class InnerJoinKTableOperation {

	public static void main(String[] args) {
		
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
		// 相当于数据库的2个持久化的表Join

        KTable<String, String> left = builder.table(KafkaHelper.USER_INFO_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("user-source").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST), Materialized.as("user-name"));

        KTable<String, String> right = builder.table(KafkaHelper.USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("address-source").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST), Materialized.as("user-address"));

        left.join(right, (userName, address) -> userName + " from " + address)
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("ktable-inner-join-ktable"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.KTABLE_JOIN_KTABLE_APP_ID)));

	}
}
