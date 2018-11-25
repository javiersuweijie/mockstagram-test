package mockstagram.stats.streams;

import mockstagram.stats.models.InfluencerStats;
import mockstagram.stats.utilities.InfluencerDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

@Component
public class RankInfluencers {

    @Value("${kafka.host}")
    private String kafkaHost;
    private String groupId = "rank-influencers";
    private KafkaConsumer<String, InfluencerStats> consumer;
    Map<Long, Long> influencers = Collections.synchronizedMap(new HashMap<>());

    @PostConstruct
    public void setup() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHost);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", InfluencerDeserializer.class.getName());

        this.consumer = new KafkaConsumer<String, InfluencerStats>(props);
        this.consumer.subscribe(Arrays.asList("influencers"));
        consumer.poll(Duration.of(0, ChronoUnit.SECONDS));
        consumer.seekToBeginning(consumer.assignment());

        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, InfluencerStats> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                for (ConsumerRecord<String, InfluencerStats> record : records) {
                    influencers.put(record.value().getPk(), -record.value().getFollowerCount());
                }
            }
        }).start();
    }

    public Long queryRank(Long pk) {
        synchronized (influencers) {
            Long followerCount = influencers.get(pk);
            TreeSet<Long> rank = influencers.values().stream()
                    .collect(Collectors.toCollection(TreeSet::new));
            Iterator<Long> iterator = rank.iterator();
            if (!iterator.hasNext()) throw new RuntimeException("Collection is empty");
            Long i = 1L;
            while (iterator.hasNext()) {
                if (iterator.next() == followerCount) {
                    return i;
                } else {
                    i++;
                }
            }
            return i;
        }
    }


}
