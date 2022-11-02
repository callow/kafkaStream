package kafka.stream.stateless.xmall;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import kafka.stream.common.JsonSerdes;
import kafka.stream.common.KafkaHelper;
import kafka.stream.stateless.xmall.model.Transaction;
import kafka.stream.stateless.xmall.model.TransactionKey;
import kafka.stream.stateless.xmall.model.TransactionPattern;
import kafka.stream.stateless.xmall.model.TransactionReward;

public class Xmallstream {

	public static void main(String[] args) {
		
		StreamsBuilder builder = new StreamsBuilder();
		//1. 从  `transaction` topic 消费原始数据: <null:Transaction>
        KStream<String, Transaction> ks0 = builder.stream(KafkaHelper.XMALL_TRANSACTION_SOURCE_TOPIC,
                Consumed.with(Serdes.String(), JsonSerdes.Transaction()).withName("transaction-source")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        
        //2. credit card number 脱敏, 仅会value进行操作，key依然是null. : <null:Transaction>
        KStream<String, Transaction> ks1 = ks0.peek((k, v) -> KafkaHelper.info("before:" + v))
                .mapValues(v -> Transaction.newBuilder(v).maskCreditCard().build(), Named.as("transaction-masking-pii"));
        
        //3. 提取出Pattern Transaction,然后Sink到Pattern Topic: <null: TransactionPattern>
        ks1.mapValues(v -> TransactionPattern.builder(v).build(), Named.as("transaction-pattern"))
                .to(KafkaHelper.XMALL_TRANSACTION_PATTERN_TOPIC, Produced.with(Serdes.String(), JsonSerdes.TransactionPattern()));
        
        //4. 提取出Reward Transaction,然后Sink到Reward Topic: <null: TransactionReward>
        ks1.mapValues(v -> TransactionReward.builder(v).build(), Named.as("transaction-reward"))
                .to(KafkaHelper.XMALL_TRANSACTION_REWARDS_TOPIC, Produced.with(Serdes.String(), JsonSerdes.TransactionReward()));
        
        //5. 过滤价格, 然后指定Key,然后Sink到Purchase Topic: <TransactionKey: Transaction>
        ks1.filter((k, v) -> v.getPrice() > 5.0D)
                .selectKey((k, v) -> new TransactionKey(v.getCustomerId(), v.getPurchaseDate()))
                .to(KafkaHelper.XMALL_TRANSACTION_PURCHASES_TOPIC, Produced.with(JsonSerdes.TransactionKey(), JsonSerdes.Transaction()));
        
        //6. 额外关注Coffee和Electrics的部门，分流一下分别Sink到 Coffee 和 Electric Topic : <null:Transaction>
        ks1.split(Named.as("transaction-split"))
                .branch((k, v) -> v.getDepartment().equals("COFFEE"),
                        Branched.withConsumer(ks -> ks.to(KafkaHelper.XMALL_TRANSACTION_COFFEE_TOPIC, Produced.with(Serdes.String(), JsonSerdes.Transaction()))))
                .branch((k, v) -> v.getDepartment().equals("ELECT"),
                        Branched.withConsumer(ks -> ks.to(KafkaHelper.XMALL_TRANSACTION_ELECT_TOPIC, Produced.with(Serdes.String(), JsonSerdes.Transaction()))));
        
        //7. 一次持久化到某个数据库
        ks1.foreach((k, v) -> KafkaHelper.info("persist:" + v));
        KafkaHelper.start(new KafkaStreams(builder.build(), KafkaHelper.config(KafkaHelper.STATELESS_XMALL_APP_ID)));

	}
}
