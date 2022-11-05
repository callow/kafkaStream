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
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;

public class SessionWindowOperation {

	public static void main(String[] args) {
        StreamsBuilder builder = KafkaHelper.streamBuilderwithStore();
        builder.stream(KafkaHelper.TRAFFIC_LOG_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.NetTrafficSerde()).withName("source")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
        .groupBy((k, v) -> v.getRemoteAddress(), Grouped.with(Serdes.String(), JsonSerdes.NetTrafficSerde()))
         // 设置会话超时为1分钟
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1)))
        .count(Named.as("session-count"), Materialized.as("session-count-state-store"))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .foreach((k, v) -> KafkaHelper.info("The user: " + k.key() + " total visits " +v +" times of website during " + k.window().start() + "-" + k.window().end()));
                
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.WINDOW_SESSION_APP_ID)));

	}
}
