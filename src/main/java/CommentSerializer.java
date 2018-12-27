import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CommentSerializer implements Serializer<Comment> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Comment data) {
        return Comment.serialize(data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Comment data) {
        return Comment.serialize(data);
    }

    @Override
    public void close() {

    }
}
