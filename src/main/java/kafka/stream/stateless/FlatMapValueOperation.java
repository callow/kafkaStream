package kafka.stream.stateless;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class FlatMapValueOperation {

	public static void main(String[] args) {

        StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
        builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        		 // value 用空格分割，大写，收集成多个
            .flatMapValues(v -> Arrays.stream(v.split("\\s+")).map(String::toUpperCase).collect(Collectors.toList()),Named.as("flatMap-values-processor"))
                
            .print(Printed.<String, String>toSysOut().withLabel("flatMapValues"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_FLAT_MAPVALUE_APP_ID)));

	}
}
