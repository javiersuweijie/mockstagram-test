package mockstagram.stats.producers;

import mockstagram.stats.models.InfluencerStats;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.LongSerializer;
import mockstagram.stats.utilities.InfluencerSerializer;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class InfluencerProducer {

    private Producer<Long, InfluencerStats> producer;
    private String topic;

    public InfluencerProducer(String bootstrapServers, String clientId, String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, InfluencerSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
        this.producer = new KafkaProducer<Long, InfluencerStats>(props);
        this.topic = topic;
    }

    public Future<RecordMetadata> send(InfluencerStats influencerStats) {
        ProducerRecord<Long, InfluencerStats> record = new ProducerRecord(topic, 0, System.currentTimeMillis(), null, influencerStats);
        return producer.send(record);
    }


}
