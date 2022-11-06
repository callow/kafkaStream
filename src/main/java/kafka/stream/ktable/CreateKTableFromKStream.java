package kafka.stream.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class CreateKTableFromKStream {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
        builder.stream(KafkaHelper.USERS_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        .toTable(Named.as("user-ktable")) // 转成Ktable
        .toStream()
        .print(Printed.<String, String>toSysOut().withLabel("kt"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.KTABLE_KSTREAM_APP_ID)));

	}
}
