package mockstagram.stats.streams;

import mockstagram.stats.models.InfluencerStats;
import mockstagram.stats.utilities.InfluencerSerde;
import mockstagram.stats.utilities.TestSerde;
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
import java.util.stream.Collectors;

@Component
public class AverageFollower {

    @Value("${kafka.host}")
    private String bootStrapServers;

    private KafkaStreams streams;
    private KafkaStreams followerStreams;
    private String topic = "influencers";
    private String applicationId = "mockstagram-average-follower";
    private String applicationIdAvr = "mockstagram-aggre-follower";
    private String averageTopic = "average-followers";

    @PostConstruct
    public void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TestSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, InfluencerStats> latestStats = builder.stream(topic, Consumed.with(Serdes.Long(), new InfluencerSerde()));

        latestStats.groupBy((key, value) -> value.getPk())
                .reduce(((value1, value2) -> value2))
                .mapValues(value -> value.getFollowerCount())
                .toStream()
                .groupByKey()
                .aggregate(ArrayList<Long>::new, (key, value, aggregator) -> {
                    aggregator.add(value);
                    return aggregator;
                },Materialized.as("FollowerCount"))
                .toStream();

        streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp();
        streams.start();

    }

    public Double queryAverageFollower() {
        ReadOnlyKeyValueStore<Long, ArrayList<Long>> readOnlySessionStore = followerStreams.store("FollowerCount", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<Long, ArrayList<Long>> iterator = readOnlySessionStore.all();
        List<Long> totalFollowers = new ArrayList<>();
        while (iterator.hasNext()) {
            return totalFollowers.stream().collect(Collectors.summingLong(Long::longValue)) / Double.valueOf(totalFollowers.size());
        }
        return 0.0;
    }
}
