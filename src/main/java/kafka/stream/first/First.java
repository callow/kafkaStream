package kafka.stream.first;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import kafka.stream.common.KafkaHelper;
public class First {
	
    public static void main(String[] args) throws InterruptedException {
        
    	// 1. create configuration
        Properties properties = KafkaHelper.config(KafkaHelper.FIRST_APP_ID);
        
        // 2. create StreamsBuilder 
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                
        		.peek((k, v) -> KafkaHelper.info("[source] value:  " + v), Named.as("pre-transform-peek"))
                
                .filter((k, v) -> v != null && v.length() > 5, Named.as("filter-processor"))
                
                .mapValues(v -> v.toUpperCase(), Named.as("map-processor"))
                
                .peek((k, v) -> KafkaHelper.info("[target] value: " + v), Named.as("post-transform-peek"))
                
                .to(KafkaHelper.FIRST_APP_TARGET_TOPIC, Produced.with(Serdes.String(), Serdes.String()).withName("sink-processor"));
        
        // 3. create topology
        Topology topology = builder.build();
        
        // 4. start stream
        KafkaHelper.start(new KafkaStreams(topology, properties));
        
    }
}
