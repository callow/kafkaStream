package kafka.stream.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class InnerJoinKStreamOperation {

	public static void main(String[] args) {
		
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
		// KStream
        KStream<String, String> stream = builder.stream(KafkaHelper.USER_INFO_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("user-source").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        // KTable
        KTable<String, String> table = builder.table(KafkaHelper.USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("address-source").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST), Materialized.as("user-address"));

        // Stream Join Table， 如果Table中有值才会往下走。
        KStream<String, String> result = stream
        		.join(table, (userName, address) -> userName + " from " + address, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        result.print(Printed.<String, String>toSysOut().withLabel("kstream-inner-join-ktable"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.KTABLE_JOIN_APP_ID)));

	}
}
