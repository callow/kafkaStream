package kafka.stream.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import kafka.stream.common.KafkaHelper;

public class SinkOperation {

	public static void main(String[] args) {
           StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();

	       KStream<String, String> ks0 = builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                   .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
           .mapValues(v -> v.toUpperCase(), Named.as("map-value-operation"));

	       // 指定序列化方式： <String:String>
	       //ks0.to(KafkaHelper.FIRST_APP_TARGET_TOPIC);
	       ks0.to(KafkaHelper.FIRST_APP_TARGET_TOPIC, Produced.with(Serdes.String(),Serdes.String()));
	       
	        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_SINK_APP_ID)));

	}
}
