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

public class CountAggregateOperation {

	public static void main(String[] args) {
		
	    StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
	    //<null, this is kafka streams>, <null, I like kafka streams>
        builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        //<this,1L>, <kafka,1L>
        .flatMap((k, v) -> Arrays.stream(v.split("\\s+")).map(e -> KeyValue.pair(e, 1L)).collect(Collectors.toList()), Named.as("flat-line"))
         // count之前先Group一下
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
         // Materialized.as() 执行存入的state store名字， 聚合会产生一个KTable
        .count(Named.as("word-count"), Materialized.as("word-count"))
        .toStream() // 将Ktable -> 转成KStream
        .print(Printed.<String, Long>toSysOut().withLabel("wc"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATEFUL_COUNT_AGGREGATE_APP_ID)));

	}
}
