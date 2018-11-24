package mockstagram.stats.utilities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class InfluencerSerializer implements Serializer {
    private ObjectMapper objectMapper;
    public InfluencerSerializer() {
        super();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            return this.objectMapper.writeValueAsBytes(data);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing influencer data");
        }
    }

    @Override
    public void close() {

    }
}
