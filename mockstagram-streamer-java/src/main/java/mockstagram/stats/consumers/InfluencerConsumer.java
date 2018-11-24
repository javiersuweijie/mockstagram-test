package mockstagram.stats.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Function;

public class InfluencerConsumer implements Runnable {

    private Consumer<String, String> consumer;
    private Function<ConsumerRecord, Void> callback;

    public InfluencerConsumer(String topic, String kafkaHost, Function<ConsumerRecord, Void> callback) {

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaHost);
        props.put("group.id", "influencer-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());



        this.consumer = new KafkaConsumer<String, String>(props);
        this.consumer.subscribe(Arrays.asList(topic));
        this.callback = callback;
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for (ConsumerRecord<String, String> record : records) {
               callback.apply(record);
            }
        }
    }

}
