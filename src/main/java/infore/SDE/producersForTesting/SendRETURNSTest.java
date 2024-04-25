package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;

import static infore.SDE.producersForTesting.Test_Return_FIN_Data.*;

public class SendRETURNSTest {

    public void run(String kafkaDataInputTopic, String topicRequests) {
        try {
            SendaddRequest(topicRequests,"cluster");
            sendFINPrices(kafkaDataInputTopic);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}