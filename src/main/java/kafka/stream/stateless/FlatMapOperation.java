package kafka.stream.stateless;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class FlatMapOperation {

	public static void main(String[] args) {
		
        StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
        builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        
        // 用空格分割，	KeyValue.pair = <kafka : 4> => 放入List
       .flatMap((k, v) -> Arrays.stream(v.split("\\s+")).map(e -> KeyValue.pair(e, e.length())).collect(Collectors.toList()),Named.as("flatMap-processor"))
       
       .print(Printed.<String, Integer>toSysOut().withLabel("flatMap"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_FLAT_MAP_APP_ID)));
	}
}
