package kafka.stream.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;

import kafka.stream.common.KafkaHelper;

public class CreateKtableFromBuilder {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
		// <alex,wang>
        builder.table(KafkaHelper.USERS_SOURCE_TOPIC, 
        		Consumed.with(Serdes.String(), Serdes.String()).withName("source")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST), 
                 Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("user-state-store"))
        .toStream()
        .print(Printed.<String, String>toSysOut().withLabel("kt"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.KTABLE_DIRECT_APP_ID)));

	}
}
