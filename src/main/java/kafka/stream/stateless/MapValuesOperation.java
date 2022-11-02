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

public class MapValuesOperation {
	
	/**
	 * 
	 * 适用于对日志进行格式化/过滤,因为只对Value做了处理
	 */

	public static void main(String[] args) {
		
        StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
        KStream<String, String> ks0 = builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        KStream<String, String> ks1 = ks0.mapValues(v -> v.toUpperCase(), Named.as("map-values-processor"));
        KStream<String, String> ks2 = ks0.mapValues((k, v) -> (k + "---" + v).toUpperCase(), Named.as("map-values-withKey-processor"));

        ks1.print(Printed.<String, String>toSysOut().withLabel("mapValues"));
        ks2.print(Printed.<String, String>toSysOut().withLabel("mapValuesWithKey"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_MAPVALUE_APP_ID)));
	}
}
