package mockstagram.stats.streams;

import mockstagram.stats.models.InfluencerStats;
import mockstagram.stats.utilities.InfluencerSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Component
public class LatestStats {

    @Value("${kafka.host}")
    private String bootStrapServers;

    private KafkaStreams streams;
    private String topic = "influencers";
    private String applicationId = "mockstagram-stats-latest";

    @PostConstruct
    public void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, InfluencerSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, InfluencerStats> latestStats = builder.stream(topic, Consumed.with(Serdes.Long(), new InfluencerSerde()));

        latestStats.groupBy((key, value) -> value.getPk())
                .reduce(((value1, value2) -> value2), Materialized.as("LatestStats"))
                .toStream();

        streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp();
        streams.start();
    }

    public InfluencerStats queryLatestStats(Long pk) {
        ReadOnlyKeyValueStore<Long, InfluencerStats> store = streams.store("LatestStats", QueryableStoreTypes.keyValueStore());
        return store.get(pk);
    }

    public Double queryAverageFollower() {
        ReadOnlyKeyValueStore<Long, InfluencerStats> store = streams.store("LatestStats", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<Long, InfluencerStats> iterator = store.all();
        List<Long> followerCounts = new ArrayList();
        while (iterator.hasNext()) {
            followerCounts.add(iterator.next().value.getFollowerCount());
        }
        return followerCounts.stream().collect(Collectors.summingLong(Long::longValue)) / Double.valueOf(followerCounts.size());
    }
}
