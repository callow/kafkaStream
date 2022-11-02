package kafka.stream.common;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

class JsonSerialization<T> implements Serializer<T> {
    final static ObjectMapper MAPPER = new ObjectMapper()
            .registerModules(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return MAPPER.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            //FIXME enhancement this source code
            return new byte[0];
        }
    }
}
