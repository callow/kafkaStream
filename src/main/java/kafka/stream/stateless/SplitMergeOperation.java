package kafka.stream.stateless;

import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

import kafka.stream.common.KafkaHelper;

public class SplitMergeOperation {

	public static void main(String[] args) {
		
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ks0 = builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        //split-stream-apache
        //split-stream-kafka
        //split-stream-streams
        //split-stream-default
        Map<String, KStream<String, String>> kStreamMap = ks0.split(Named.as("split-stream-"))
                .branch((k, v) -> v.contains("apache"), Branched.as("apache"))
                .branch((k, v) -> v.contains("kafka"), Branched.as("kafka"))
                // 创建流的时候顺便transform一下，e.g: Branched.withFunction => 顺便大写！ 
                .branch((k, v) -> v.contains("clickhouse"), Branched.withFunction(ks -> ks.mapValues(e -> e.toUpperCase()), "clickhouse"))
                .defaultBranch(Branched.as("default"));
        
        // 看一下 split-stream-streams 这个流，会中断流
        kStreamMap.get("split-stream-clickhouse").print(Printed.<String, String>toSysOut().withLabel("clickhouse"));
        
        // Merge一下 split-stream-Apache 和 split-stream-default 成一个大流
        kStreamMap.get("split-stream-apache")
        	.merge(kStreamMap.get("split-stream-default"), Named.as("merge-processor"))
        	.print(Printed.<String, String>toSysOut().withLabel("merge"));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_SPLIT_MERGE_APP_ID)));

        
	}
}
