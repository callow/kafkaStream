package kafka.stream.stateful;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.StreamJoined;

import kafka.stream.common.KafkaHelper;

public class LeftJoinOperation {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
		
	    //<null,001,Alex>
        KStream<String, String> ks0 = builder.stream(KafkaHelper.USER_INFO_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("user-info").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        // 分配KEY: <001,001,Alex>
        KStream<String, String> userKS = ks0.map((k, v) -> KeyValue.pair(v.split(",")[0], v), Named.as("user-info-transform"));
        
        //<null,001,CN>
        KStream<String, String> ks1 = builder.stream(KafkaHelper.USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("user-address").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        //分配KEY: <001,001,CN>
        KStream<String, String> addressKS = ks1.map((k, v) -> KeyValue.pair(v.split(",")[0], v), Named.as("user-addr-transform"));
        
        userKS.leftJoin(addressKS, 
        		(left, right) -> left + "----" + right, JoinWindows.of(Duration.ofMinutes(1)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
        .print(Printed.<String, String>toSysOut().withLabel("left-join"));
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATEFUL_LEFT_JOIN_APP_ID)));

	}
}
