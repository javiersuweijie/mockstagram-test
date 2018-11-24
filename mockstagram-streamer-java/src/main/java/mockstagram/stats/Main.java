package mockstagram.stats;

import com.fasterxml.jackson.databind.ObjectMapper;
import mockstagram.stats.consumers.InfluencerConsumer;
import mockstagram.stats.models.InfluencerStats;
import mockstagram.stats.producers.InfluencerProducer;
import mockstagram.stats.producers.PKProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import static org.asynchttpclient.Dsl.asyncHttpClient;

@SpringBootApplication
public class Main {
    private static Logger log = LoggerFactory.getLogger(Main.class);
    private static Integer maxConnections = 10000;

    public static void main (String[] args) throws Exception {
        SpringApplication app = new SpringApplication(Main.class);
        Environment env = app.run(args).getEnvironment();
        log.info("\n----------------------------------------------------------\n\t" +
                        "Application '{}' is running! Access URLs:\n\t" +
                        "Local: \t\thttp://localhost:{}\n\t" +
                        "External: \thttp://{}:{}\n----------------------------------------------------------",
                env.getProperty("spring.application.name"),
                env.getProperty("server.port"),
                InetAddress.getLocalHost().getHostAddress(),
                env.getProperty("server.port"));


        Integer pkPartitions = Integer.valueOf(Optional.of(env.getProperty("kafka.partitions")).orElse("2"));
        String kafkaHost     = Optional.of(env.getProperty("kafka.host")).orElse("localhost:9092");
        String url           = Optional.of(env.getProperty("mockstagram.url")).orElse("http://localhost:3000/api/v1/influencers/");


        InfluencerProducer influencerProducer = new InfluencerProducer(kafkaHost, "influencerProducer", "influencers");
        PKProducer pkProducer = new PKProducer(kafkaHost, "pkProducer", "pk", pkPartitions);

        ObjectMapper objectMapper = new ObjectMapper();
        AsyncHttpClient asyncHttpClient = asyncHttpClient(new DefaultAsyncHttpClientConfig.Builder().setMaxConnections(maxConnections));
        BlockingQueue<Map.Entry<String, Future<Response>>> futures = new LinkedBlockingQueue<>();
        BlockingQueue<String> requestQueue = new LinkedBlockingQueue<>(maxConnections);

        InfluencerConsumer influencerConsumer = new InfluencerConsumer(
                "pk",
                kafkaHost,
                (record) -> {
                    String pk = String.valueOf(record.value());
                    requestQueue.add(pk);
                    futures.offer(new AbstractMap.SimpleEntry(pk, asyncHttpClient.prepareGet(url + pk).execute()));
                    return null;
                });

        new Thread(influencerConsumer).start();

        new Thread(() -> {
            while (true) {
                try {
                    Map.Entry<String, Future<Response>> entry = futures.take();
                    try {
                        Future<Response> future = entry.getValue();
                        if (future.isDone()) {
                            requestQueue.take();
                            byte[] body = future.get().getResponseBodyAsBytes();
                            InfluencerStats influencerStats = objectMapper.readValue(body, InfluencerStats.class);
                            influencerProducer.send(influencerStats);
                            pkProducer.send(String.valueOf(influencerStats.getPk()));
                        } else {
                            futures.offer(entry);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        requestQueue.add(entry.getKey());
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

}
