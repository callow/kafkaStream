package kafka.stream.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import kafka.stream.stateless.xmall.model.Transaction;
import kafka.stream.stateless.xmall.model.TransactionKey;
import kafka.stream.stateless.xmall.model.TransactionPattern;
import kafka.stream.stateless.xmall.model.TransactionReward;

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
}
