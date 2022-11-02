package kafka.stream.stateful;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import kafka.stream.common.KafkaHelper;

public class TransformOperation {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithStore();
		
        //<null,hello kafka>
        KStream<String, String> ks0 = builder.stream(KafkaHelper.FIRST_APP_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        
        // <hello,hello> <kafka,kafka> 组成新的Key:value
        ks0.flatMap((k, v) -> Arrays.stream(v.split("\\s+")).map(e -> KeyValue.pair(e, e)).collect(Collectors.toList()), Named.as("flat-map-processor"))
        
        // shuffle一下，将数据shuffle一下根据key分到不同的partition
        .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
        // stateful操作： <k,v,kr> , k,v是原始的数据类型，kr是返回的数据类型
        .transform(() -> new Transformer<String, String, KeyValue<String, Integer>>() {
            private KeyValueStore<String, Integer> keyValueStore;
            @Override
            public void init(ProcessorContext processorContext) {
                this.keyValueStore = processorContext.getStateStore(KafkaHelper.STATE_STORE_NAME);
            }
            @Override
            public KeyValue<String, Integer> transform(String key, String value) {
                Integer count = keyValueStore.get(key);
                if (count == null || count == 0) {
                    count = 1;
                } else {
                    count++;
                }
                keyValueStore.put(key, count);
                return KeyValue.pair(key, count);
            }

            @Override public void close() {}
        }, Named.as("stateful-transform-processor"), KafkaHelper.STATE_STORE_NAME)
        
        .peek((k, v) -> KafkaHelper.info("key:" + k + "value:" + v))
        .to(KafkaHelper.FIRST_APP_TARGET_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()));
        
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATEFUL_TRANSFORM_APP_ID)));

	}
}
