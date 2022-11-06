package kafka.stream.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.ktable.model.Shoot;
import kafka.stream.ktable.model.ShootStats;

public class TransformValuesOperation {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithShootStore();

		KTable<String, Shoot> table = builder.table(KafkaHelper.SHOOT_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.ShootSerde()).withName("source")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        table.transformValues(() -> new ValueTransformerWithKey<String, Shoot, ShootStats>() {

                    private KeyValueStore<String, ShootStats> keyValueStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.keyValueStore = context.getStateStore(KafkaHelper.SHOOT_STATE_STORE);
                    }

                    @Override
                    public ShootStats transform(String readOnlyKey, Shoot value) {
                    	KafkaHelper.info("key:" + readOnlyKey + " value:" + value);
                        if (value == null) {
                            return ShootStats.newBuilder(readOnlyKey).build();
                        }

                        ShootStats shootStats = this.keyValueStore.get(readOnlyKey);
                        if (shootStats == null) {
                            shootStats = ShootStats.newBuilder(value).build();
                        } else if (shootStats.getStatus().equals("FINISHED")) {
                            return shootStats;
                        } else if (shootStats.getCount() == 10) {
                            shootStats.setStatus("FINISHED");
                        } else {
                            shootStats.setCount(shootStats.getCount() + 1);
                            shootStats.setLastScore(value.getScore());
                            shootStats.setBestScore(Math.max(value.getScore(), shootStats.getBestScore()));
                        }
                        this.keyValueStore.put(readOnlyKey, shootStats);
                        return shootStats;
                    }

                    @Override
                    public void close() {

                    }
                }, KafkaHelper.SHOOT_STATE_STORE)
        		.toStream()
        		.filter((k, v) -> v.getBestScore() >= 8)
        		.filterNot((k, v) -> v.getStatus().equals("FINISHED"))
                .print(Printed.<String, ShootStats>toSysOut().withLabel("shooting-game"));
	}
}
