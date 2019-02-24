import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class NYTConsumer {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "comment_consumer2");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");


        double start = System.nanoTime();
        AtomicInteger counter = new AtomicInteger(0);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            //TopicPartition partition0 = new TopicPartition("snippet", 0);
            consumer.subscribe(Collections.singletonList("comment12"));
            int c = 0;
            for (int i=0; i < 1000; i++) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(5));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (c % 10000 == 0) {
                        System.out.println(Comment.deserialize(record.value()).getCommentBody());
                    }
                    c++;
                    counter.incrementAndGet();
                }
            }
        }
        double end = System.nanoTime();
        System.out.println("number of comments : " + counter.get());
        System.out.println((end - start) / 1000000000 + "s");
    }
}
