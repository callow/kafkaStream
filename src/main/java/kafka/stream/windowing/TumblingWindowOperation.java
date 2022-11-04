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
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;

public class TumblingWindowOperation {

	/**
	 * 检测网络流量，如果每分钟来10次，直接预警
	 */
	public static void main(String[] args) {
		   StreamsBuilder builder = KafkaHelper.streamBuilderwithStore();
	       builder.stream(KafkaHelper.TRAFFIC_LOG_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.NetTrafficSerde())
	                        .withName("source-processor").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
	        		 // 先按访问IP地址分组 <123.3.4.123: Traffic>
	                .groupBy((k, v) -> v.getRemoteAddress(), Grouped.with(Serdes.String(), JsonSerdes.NetTrafficSerde()))
	                 // ofSizeWithNoGrace(1), Grace = 给一点点Buffer, 每个消息一分钟之内进行再分组（window）
	                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
	                 // 在每一个window内进行counter
	                .count(Named.as("tumbling-count"), Materialized.as("tumbling-count-state-store"))
	                .filter((k, v) -> v >= 10)
	                .toStream()
	                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("attack"));
	       
	        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.WINDOW_TUMBLING_APP_ID)));

	}
}
