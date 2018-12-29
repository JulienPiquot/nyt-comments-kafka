import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CommentStats {

    public static void readLocalStore(KafkaStreams streams) {
        ReadOnlyKeyValueStore<String, Long> countStore = streams.store("counts-store", QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<String, Long> recommandationStore = streams.store("recommandation-store", QueryableStoreTypes.keyValueStore());

        System.out.println("number of counts : " + countStore.approximateNumEntries());
        System.out.println("number of recommandations : " + recommandationStore.approximateNumEntries());

        // Get the values for all of the keys available in this application instance
        KeyValueIterator<String, Long> range = countStore.all();
        int i = 0;
        while (range.hasNext()) {
            KeyValue<String, Long> next = range.next();
            Long recoSum = recommandationStore.get(next.key);
            if (recoSum != null) {
                System.out.println(i + " - count for " + next.key + ": " + next.value + " - recommandation avg is: " + recoSum / next.value);
            } else {
                System.out.println("no reco sum for " + next.key);
            }
            i++;
        }
    }

    public static void main(String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CommentSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Comment> source = builder.stream("comments");

        KGroupedStream<String, Comment> kgStream = source.groupBy((key, value) -> value.getArticleID());
        Aggregator<String, Comment, Long> agg = (key, value, aggregate) -> value.getRecommandations() + aggregate;
        Initializer<Long> init = () -> 0L;
        kgStream.aggregate(init, agg, Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("recommandation-store").withValueSerde(Serdes.Long()));
        kgStream.count(Materialized.as("counts-store"));

        // need to override value serde to Long type
        //counts.toStream().to("comments-count", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        System.out.println(topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                readLocalStore(streams);
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
        }
    }
}

