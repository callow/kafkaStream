package kafka.stream.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import kafka.stream.stateful.sales.model.Sales;
import kafka.stream.stateful.sales.model.SalesStats;
import kafka.stream.stateless.xmall.model.Transaction;
import kafka.stream.stateless.xmall.model.TransactionKey;
import kafka.stream.stateless.xmall.model.TransactionPattern;
import kafka.stream.stateless.xmall.model.TransactionReward;
import kafka.stream.windowing.model.NetTraffic;

public class JsonSerdes {

    public static TransactionPatternWrapSerde TransactionPattern() {
        return new TransactionPatternWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionPattern.class));
    }

    public final static class TransactionPatternWrapSerde extends WrapSerde<TransactionPattern> {
        private TransactionPatternWrapSerde(Serializer<TransactionPattern> serializer, Deserializer<TransactionPattern> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static TransactionRewardWrapSerde TransactionReward() {
        return new TransactionRewardWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionReward.class));
    }

    public final static class TransactionRewardWrapSerde extends WrapSerde<TransactionReward> {
        private TransactionRewardWrapSerde(Serializer<TransactionReward> serializer, Deserializer<TransactionReward> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static TransactionKeyWrapSerde TransactionKey() {
        return new TransactionKeyWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionKey.class));
    }

    public final static class TransactionKeyWrapSerde extends WrapSerde<TransactionKey> {
        private TransactionKeyWrapSerde(Serializer<TransactionKey> serializer, Deserializer<TransactionKey> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static TransactionWrapSerde Transaction() {
        return new TransactionWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(Transaction.class));
    }

    public final static class TransactionWrapSerde extends WrapSerde<Transaction> {
        private TransactionWrapSerde(Serializer<Transaction> serializer, Deserializer<Transaction> deserializer) {
            super(serializer, deserializer);
        }
    }
    
    public static SalesWrapSerde SalesSerde() {
        return new SalesWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(Sales.class));
    }

    public final static class SalesWrapSerde extends WrapSerde<Sales> {
        private SalesWrapSerde(Serializer<Sales> serializer, Deserializer<Sales> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static SalesStatsWrapSerde SalesStatsSerde() {
        return new SalesStatsWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(SalesStats.class));
    }

    public final static class SalesStatsWrapSerde extends WrapSerde<SalesStats> {
        private SalesStatsWrapSerde(Serializer<SalesStats> serializer, Deserializer<SalesStats> deserializer) {
            super(serializer, deserializer);
        }
    }
    
    public static NetTrafficWrapSerde NetTrafficSerde() {
        return new NetTrafficWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(NetTraffic.class));
    }

    public final static class NetTrafficWrapSerde extends WrapSerde<NetTraffic> {
        private NetTrafficWrapSerde(Serializer<NetTraffic> serializer, Deserializer<NetTraffic> deserializer) {
            super(serializer, deserializer);
        }
    }
}
