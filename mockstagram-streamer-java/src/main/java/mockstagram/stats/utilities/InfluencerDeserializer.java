package mockstagram.stats.utilities;

import com.fasterxml.jackson.databind.ObjectMapper;
import mockstagram.stats.models.InfluencerStats;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class InfluencerDeserializer implements Deserializer {

    private ObjectMapper objectMapper;

    public InfluencerDeserializer() {
        super();
        objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map map, boolean b) {

    }


    @Override
    public Object deserialize(String topic, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, InfluencerStats.class);
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    @Override
    public void close() {

    }
}
