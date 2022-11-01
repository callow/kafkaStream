package kafka.stream.stateless;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class MapOperation {

	public static void main(String[] args) {
		Properties properties = KafkaHelper.config(KafkaHelper.STATELESS_APP_ID);
		
		StreamsBuilder builder = new StreamsBuilder();
	    KStream<String, String> ks0 = builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
	                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
	     
	    // KeyValue.pair(k, v) 会新建一个Key:value 对
	    KStream<String, Integer> ks1 = ks0.map((k,v) -> KeyValue.pair(v, v.length()), Named.as("map-transform-processor"));
	     
	    // Printed.toSysOut(), 在Stream中进行控制台输出
	    ks1.print(Printed.<String, Integer>toSysOut().withLabel("map-operation"));
	     
        KafkaHelper.start(new KafkaStreams(builder.build(), properties));
		
	}
}
