package infore.SDE.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import infore.SDE.messages.Message;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.nio.charset.StandardCharsets;

public class KafkaProducerMessage {

    private final FlinkKafkaProducer<Message> myProducer;

    public KafkaProducerMessage(String brokerList, String outputTopic) {
        this.myProducer = new FlinkKafkaProducer<>(
                brokerList,
                outputTopic,
                new MessageSerializer()
        );
        this.myProducer.setWriteTimestampToKafka(false);
    }

    public SinkFunction<Message> getProducer() {
        return myProducer;
    }
}

class MessageSerializer implements KeyedSerializationSchema<Message> {
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serializeKey(Message element) {
        // If you want a key, you can derive one from element; otherwise null is acceptable.
        return null;
    }

    @Override
    public byte[] serializeValue(Message element) {
        try {
            // Convert the entire Message object to a JSON string (including enum + map/string).
            String json = element.toJsonString();
            return json.getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            // Handle or rethrow serialization exceptions
            throw new RuntimeException("Error serializing Message to JSON", e);
        }
    }

    @Override
    public String getTargetTopic(Message element) {
        // Returning null indicates that we'll use the default topic.
        // If you want to dynamically route to topics, return the name here.
        return null;
    }
}
