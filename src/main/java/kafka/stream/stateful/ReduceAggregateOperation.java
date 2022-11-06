package kafka.stream.stateful;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class ReduceAggregateOperation {

	public static void main(String[] args) {
		
	    StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();

	    //<null, this is kafka streams>
        //<null, I like kafka streams>
        builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        //<this,1L>, <kafka,1L>
        .flatMap((k, v) -> Arrays.stream(v.split("\\s+")).map(e -> KeyValue.pair(e, 1L)).collect(Collectors.toList()), Named.as("flat-line"))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
        // .reduce((aggregated,curVal) -> aggregated + curVal )
        .reduce(Long::sum, Named.as("reduce-processor"), Materialized.as("reduce-state-store"))
        .toStream()
        .print(Printed.<String, Long>toSysOut().withLabel("wc"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATEFUL_REDUCE_AGGREGATE_APP_ID)));

	}
}
