package infore.SDE.sources;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class KafkaStringConsumer {

    private FlinkKafkaConsumer<String> fc;

    /**
     * Constructor for building Kafka Consumer module
     * without starting from the earliest info point possible
     * @param server FQDN/IP name of Kafka instance
     * @param topic Kafka topic to consume from
     */
    public KafkaStringConsumer(String server, String topic) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", "SDE");

        fc = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties).setStartFromEarliest();
    }

    /**
     * Constructor for building Kafka Consumer module
     * which starts consuming from the earliest info point possible
     * @param server FQDN/IP name of Kafka instance
     * @param topic Kafka topic to consume from
     * @param startFromEarliest TRUE/FALSE (TRUE: start from earliest point possible/ FALSE: simple consumer (edge case))
     */
    public KafkaStringConsumer(String server, String topic, boolean startFromEarliest) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", "SDE");

        if(startFromEarliest){
            fc = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties).setStartFromEarliest();
        }else{
            fc = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        }

    }
    public void cancel() {

        fc.cancel();

    }

    public FlinkKafkaConsumer<String> getFc() {
        return fc;
    }

    public void setFc(FlinkKafkaConsumer<String> fc) {
        this.fc = fc;
    }
}
