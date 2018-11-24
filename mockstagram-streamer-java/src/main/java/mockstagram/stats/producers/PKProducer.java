package mockstagram.stats.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class PKProducer {

    private Producer<Long, String> producer;
    private String topic;
    private Integer partitions;

    public PKProducer(String bootstrapServers, String clientId, String topic, Integer partitions) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
        this.producer = new KafkaProducer<Long, String>(props);
        this.topic = topic;
        this.partitions = partitions;
    }

    public Future<RecordMetadata> send(String pk) {
        ProducerRecord<Long, String> record = new ProducerRecord(topic, Integer.valueOf(pk) % partitions, System.currentTimeMillis(), null, pk);
        return producer.send(record);
    }
}
