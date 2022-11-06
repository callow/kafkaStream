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
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;


public class HoppingWindowOperation {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
		
        builder.stream(KafkaHelper.TRAFFIC_LOG_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.NetTrafficSerde()).withName("source")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                 // 每一个page放一个partition
        		.groupBy((k, v) -> v.getPage(), Grouped.with(Serdes.String(), JsonSerdes.NetTrafficSerde()))
        		 // 每10秒往前跳一次，跳的步伐是0，window 大小是1
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(0)).advanceBy(Duration.ofSeconds(10)))
                .count(Named.as("hopping-count"), Materialized.as("hopping-count-state-store"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .foreach((k, v) -> KafkaHelper.info(k.window().start() + " - " +  k.window().end() + " for website: " + k.key() + ",access total " + v + "in past 1 minute" ));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.WINDOW_HOPPING_APP_ID)));
	}
}
