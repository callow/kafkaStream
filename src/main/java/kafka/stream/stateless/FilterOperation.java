package kafka.stream.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class FilterOperation {

	public static void main(String[] args) {

		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
        KStream<String, String> ks0 = builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        ks0.filter((k, v) -> v.contains("kafka"), Named.as("filter-processor"))
                .print(Printed.<String, String>toSysOut().withLabel("filtering"));

        ks0.filterNot((k, v) -> v.contains("kafka"), Named.as("filter-not-processor"))
                .print(Printed.<String, String>toSysOut().withLabel("filtering-not"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_FILTER_APP_ID)));
	}
}
