import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Properties;

public class NYTProducer {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        System.out.println("Hello World ! Starting producing articles");
        List<Article> articles = Article.readArticles().subList(20, 30);
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (Article article : articles) {
                System.out.println("produce : " + article.getSnippet());
                producer.send(new ProducerRecord<>("test", article.getHeadline()));
            }
        }
    }
}
