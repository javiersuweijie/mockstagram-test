package mockstagram.stats;

import com.fasterxml.jackson.databind.ObjectMapper;
import mockstagram.stats.consumers.InfluencerConsumer;
import mockstagram.stats.models.InfluencerStats;
import mockstagram.stats.producers.InfluencerProducer;
import mockstagram.stats.producers.PKProducer;
import mockstagram.stats.utilities.Tuple;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.asynchttpclient.Dsl.asyncHttpClient;

@SpringBootApplication
public class Main {
    private static Logger log = LoggerFactory.getLogger(Main.class);
    private static Integer maxConnections = 250;

    public static void main (String[] args) throws Exception {
        SpringApplication app = new SpringApplication(Main.class);
        Environment env = app.run(args).getEnvironment();
        AtomicLong count = new AtomicLong();
        AtomicLong errorCount = new AtomicLong();
        log.info("\n----------------------------------------------------------\n\t" +
                        "Application '{}' is running! Access URLs:\n\t" +
                        "Local: \t\thttp://localhost:{}\n\t" +
                        "External: \thttp://{}:{}\n----------------------------------------------------------",
                env.getProperty("spring.application.name"),
                env.getProperty("server.port"),
                InetAddress.getLocalHost().getHostAddress(),
                env.getProperty("server.port"));


        Integer pkPartitions = Integer.valueOf(Optional.of(env.getProperty("kafka.partitions")).orElse("2"));
        String  kafkaHost    = Optional.of(env.getProperty("kafka.host")).orElse("localhost:9092");
        String  url          = Optional.of(env.getProperty("mockstagram.url")).orElse("http://localhost:3000/api/v1/influencers/");


        InfluencerProducer influencerProducer = new InfluencerProducer(kafkaHost, "influencerProducer", "influencers");
        PKProducer pkProducer = new PKProducer(kafkaHost, "pkProducer", "pk", pkPartitions);

        ObjectMapper objectMapper = new ObjectMapper();
        AsyncHttpClient asyncHttpClient = asyncHttpClient(new DefaultAsyncHttpClientConfig.Builder()
                .setMaxConnections(maxConnections)
                .setConnectTimeout(60000)
                .setRequestTimeout(60000));

        BlockingQueue<Tuple<String, Future<Response>>> futures = new LinkedBlockingQueue<>();
        BlockingQueue<String> requestQueue = new LinkedBlockingQueue(maxConnections);

        /* The InfluencerConsumer takes a callback to
           1. Take a record
           2. Create a async request if not > maxConnections
           3. Adds the future to a queue for processing in another loop
           4. Takes the pk and re-publish on the pk topic to be queued again
         */
        InfluencerConsumer influencerConsumer = new InfluencerConsumer(
                "pk",
                kafkaHost,
                (record) -> {
                    try {
                        String pk = String.valueOf(record.value());
                        requestQueue.put(pk);
                        futures.put(new Tuple(pk, asyncHttpClient.prepareGet(url + pk).execute()));
                        pkProducer.send(String.valueOf(pk));
                        return null;
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                        return null;
                    }
                });

        new Thread(influencerConsumer).start();

        /* Loop to:
         1. Take requests from internal queue
         2. Check if they are done
         3.     If done, publish message on another topic
                Else, add back to the queue

         Blocks if the request queue is empty
         */
        new Thread(() -> {
            while (true) {
                try {
                    Tuple<String, Future<Response>> entry = futures.take();
                    try {
                        Future<Response> future = entry.getValue();
                        if (future.isDone()) {
                            requestQueue.take();
                            byte[] body = future.get().getResponseBodyAsBytes();
                            InfluencerStats influencerStats = objectMapper.readValue(body, InfluencerStats.class);
                            influencerProducer.send(influencerStats);
                            count.incrementAndGet();
                        } else {
                            futures.offer(entry);
                        }
                    } catch (Exception e) {
//                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        /* Loop to print process stats every 5 seconds
         */
        new Thread( () -> {
            while(true) {
                try {
                    Thread.sleep(5000);
                    log.info("Processed: {}, Error: {}, request queue size: {}, futures size: {}", count, errorCount, requestQueue.size(), futures.size());
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

}
