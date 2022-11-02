package kafka.stream.stateful.xmall;

import java.util.Collections;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.stateless.xmall.model.Transaction;
import kafka.stream.stateless.xmall.model.TransactionReward;

public class XmallStream {

	public static void main(String[] args) {
		StreamsBuilder builder = KafkaHelper.streamBuilderwithoutStore();
	       KStream<String, Transaction> ks0 = builder.stream(KafkaHelper.XMALL_TRANSACTION_SOURCE_TOPIC,
	                Consumed.with(Serdes.String(), JsonSerdes.Transaction()).withName("transaction-source")
	                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
	       
	      // 1. 累加Reward点，然后写出，注意加入Key, 然后transformValues
	       ks0.mapValues(v -> TransactionReward.builder(v).build(), Named.as("transaction-reward"))
	                .selectKey((k, v) -> v.getCustomerId())
	                .repartition(Repartitioned.with(Serdes.String(), JsonSerdes.TransactionReward()))
	                .transformValues(new ValueTransformerSupplier<TransactionReward, TransactionReward>() {
	                    @Override
	                    public ValueTransformer<TransactionReward, TransactionReward> get() {
	                        return new ValueTransformer<TransactionReward, TransactionReward>() {
	                            private KeyValueStore<String, Integer> keyValueStore;
	                            @Override
	                            public void init(ProcessorContext processorContext) {
	                                this.keyValueStore = processorContext.getStateStore(KafkaHelper.STATE_STORE_NAME);
	                            }
	                            @Override
	                            public TransactionReward transform(TransactionReward reward) {
	                                Integer rewardPoints = keyValueStore.get(reward.getCustomerId());
	                                if (rewardPoints == null || rewardPoints == 0) {
	                                    rewardPoints = reward.getRewardPoints();
	                                } else {
	                                    rewardPoints += reward.getRewardPoints();
	                                }
	                                keyValueStore.put(reward.getCustomerId(), rewardPoints);
	                                // 下游还需要新的reward把这个totalPoint返回回去
	                                TransactionReward newTransactionReward = TransactionReward.builder(reward).build();
	                                newTransactionReward.setTotalPoints(rewardPoints);
	                                return newTransactionReward;
	                            }
	                            @Override
	                            public void close() {}};
	                    }
	                    @Override
	                    public Set<StoreBuilder<?>> stores() {
	                    	// 有个这里，一开始的builder就不需要add了。因此使用了streamBuilderwithoutStore
	                        return Collections.singleton(KafkaHelper.STRING_INT_STOREBUILDER);
	                    }
	                }, Named.as("total-rewards-points-processor"))
	                .to(KafkaHelper.XMALL_TRANSACTION_REWARDS_TOPIC, 
	                		Produced.with(Serdes.String(), JsonSerdes.TransactionReward()));
		
		
	}
}
