import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CommentSerde implements Serde<Comment> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Comment> serializer() {
        return new CommentSerializer();
    }

    @Override
    public Deserializer<Comment> deserializer() {
        return new CommentDeserializer();
    }
}
