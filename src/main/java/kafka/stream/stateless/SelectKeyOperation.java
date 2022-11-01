package kafka.stream.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class SelectKeyOperation {

	public static void main(String[] args) {
		
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(KafkaHelper.STATELESS_SELECT_KEY_APP_ID, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        	// 用Value作为Key
            .selectKey((nokey, v) -> v, Named.as("selectKey-processor"))
            .print(Printed.<String, String>toSysOut().withLabel("selectKey"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_SELECT_KEY_APP_ID)));

	}
}
