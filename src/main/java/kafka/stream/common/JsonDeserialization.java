package kafka.stream.common;

import static kafka.stream.common.JsonSerialization.MAPPER;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JsonDeserialization<T> implements Deserializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonDeserialization.class);

    private Class<T> deserializerClass;

    public JsonDeserialization(Class<T> deserializerClass) {
        this.deserializerClass = deserializerClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return MAPPER.readValue(data, deserializerClass);
        } catch (Exception e) {
            //FIXME
            LOG.error("deserialize failed, ignore the error.");
        }
        return null;
    }
}
