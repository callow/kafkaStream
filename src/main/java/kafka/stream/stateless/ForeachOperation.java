package kafka.stream.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import kafka.stream.common.KafkaHelper;

public class ForeachOperation {

	public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        
        // 这里返回void， 即中断，无法进行进一步操作，比如print
        .foreach((k, v) -> KafkaHelper.info(k + "----" + v));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_FOREACH_APP_ID)));

	}
}
