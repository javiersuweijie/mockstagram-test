package mockstagram.stats.utilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LongArraySerde implements Serde {

    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return new Serializer() {
            @Override
            public void configure(Map configs, boolean isKey) {

            }

            @Override
            public byte[] serialize(String topic, Object data) {
                try {
                    return objectMapper.writeValueAsBytes(data);
                }
                catch (JsonProcessingException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Error serializing Long Array");
                }
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public Deserializer deserializer() {
        return new Deserializer() {
            @Override
            public void configure(Map configs, boolean isKey) {

            }

            @Override
            public Object deserialize(String topic, byte[] data) {
                try {
                    return objectMapper.readValue(data, new TypeReference<List<Long>>() {
                    });
                }
                catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Error deserializing Long Array");
                }
            }

            @Override
            public void close() {

            }
        };
    }
}
