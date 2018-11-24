package mockstagram.stats.streams;

import mockstagram.stats.models.InfluencerStats;
import mockstagram.stats.utilities.InfluencerDeserializer;
import mockstagram.stats.utilities.InfluencerSerde;
import mockstagram.stats.utilities.InfluencerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Component
public class AggregateStatsByTimeWindow {

    @Value("${kafka.host}")
    private String bootStrapServers;

    private KafkaStreams streams;
    private String topic = "influencers";
    private String applicationId = "mockstagram-stats";

    @PostConstruct
    public void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, InfluencerSerde.class.getName());
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        StreamsBuilder builder = new StreamsBuilder();
        Serde<InfluencerStats> serde = Serdes.serdeFrom(new InfluencerSerializer(), new InfluencerDeserializer());
        KStream<Long, InfluencerStats> influencerStatsStream = builder.stream(topic, Consumed.with(Serdes.Long(), serde));
        influencerStatsStream
                .groupBy((key, value) -> value.getPk())
                .windowedBy(TimeWindows.of(60000))
                .aggregate(InfluencerStats::new, (key, value, aggregate) -> {
                    if (aggregate.getPk() == null) {
                        return value;
                    } else {
                        return aggregate;
                    }
                }, Materialized.<Long, InfluencerStats, WindowStore<Bytes, byte[]>>as("FollowerStats"))
                .toStream();
        streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp();
        streams.start();
    }

    public List<InfluencerStats> queryInfluencerStat(Long pk) {
        ReadOnlyWindowStore<Long, InfluencerStats> windowStore = streams.store("FollowerStats", QueryableStoreTypes.windowStore());
        WindowStoreIterator<InfluencerStats> iterator = windowStore.fetch(pk, 0L, Instant.now().toEpochMilli());
        List<InfluencerStats> influencerStats = new ArrayList<>();
        while (iterator.hasNext()) {
            influencerStats.add(iterator.next().value);
        }
        iterator.close();
        return influencerStats;
    }


}
