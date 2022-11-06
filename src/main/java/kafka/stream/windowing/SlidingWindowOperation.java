package kafka.stream.windowing;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Suppressed;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;

public class SlidingWindowOperation {

	public static void main(String[] args) {
		
        StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
        builder.stream(KafkaHelper.TRAFFIC_LOG_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.NetTrafficSerde()).withName("source")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .groupBy((k, v) -> v.getPage(), Grouped.with(Serdes.String(), JsonSerdes.NetTrafficSerde()))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(10)))
                .count(Named.as("sliding-count"), Materialized.as("sliding-count-state-store"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .foreach((k, v) -> KafkaHelper.info(k.window().start() + " - " +  k.window().end() + " for website: " + k.key() + ",access total " + v + "in past 1 minute" ));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.WINDOW_SLIDING_APP_ID)));
	}
}
