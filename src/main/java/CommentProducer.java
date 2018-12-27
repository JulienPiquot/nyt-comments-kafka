import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class CommentProducer {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (Producer<String, byte[]> producer = new KafkaProducer<>(props)) {
            Comment.commentStream().forEach(comment -> {
                //System.out.println(comment.getCommentBody());
                producer.send(new ProducerRecord<>("comments", Comment.serialize(comment)));
            });
        }
    }
}
