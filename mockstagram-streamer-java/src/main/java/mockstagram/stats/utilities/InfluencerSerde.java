package mockstagram.stats.utilities;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class InfluencerSerde implements Serde {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return new InfluencerSerializer();
    }

    @Override
    public Deserializer deserializer() {
        return new InfluencerDeserializer();
    }
}
