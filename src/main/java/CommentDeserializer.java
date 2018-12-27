import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CommentDeserializer implements Deserializer<Comment> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Comment deserialize(String topic, byte[] data) {
        return Comment.deserialize(data);
    }

    @Override
    public void close() {

    }

    @Override
    public Comment deserialize(String topic, Headers headers, byte[] data) {
        return Comment.deserialize(data);
    }
}
