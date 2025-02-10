package infore.SDE.reduceFunctions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Message;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessageReducer extends KeyedProcessFunction<String, Message, Message> {

    // This state will hold all messages for a given relatedRequestIdentifier.
    private transient ListState<Message> messagesState;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Message> descriptor =
                new ListStateDescriptor<>("messagesState", Message.class);
        messagesState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(Message value, Context ctx, Collector<Message> out) throws Exception {
        // Add the incoming message to state.
        messagesState.add(value);

        // Retrieve all messages for this key.
        List<Message> currentMessages = new ArrayList<>();
        for (Message msg : messagesState.get()) {
            currentMessages.add(msg);
        }

        // Check if we have collected the expected number of messages.
        // (Assuming all messages for the same request have the same parallelism value.)
        if (currentMessages.size() >= value.getParallelism()) {
            // Declare a variable to hold aggregated content.
            Object aggregatedContents = null;

            // If the requestID is 1000, choose the message with the longest content.
            if (value.getRequestTypeID() == 1000) {
                // Assuming getContent returns a String (or an object that makes sense to convert to String).
                Message messageWithLongestContent = currentMessages.stream()
                        .filter(msg -> msg.getContent() != null)
                        .max(Comparator.comparingInt(msg -> msg.getContent().toString().length()))
                        .orElse(null);

                aggregatedContents = messageWithLongestContent.getContent();

            } else {
                // For all other request types, aggregate the content values into a list.
                List<Object> aggregatedList = currentMessages.stream()
                        .map(Message::getContent)
                        .collect(Collectors.toList());
                aggregatedContents = aggregatedList;

            }

            // Create an aggregated message.
            Message aggregatedMessage = new Message();
            aggregatedMessage.setRelatedRequestIdentifier(value.getRelatedRequestIdentifier());
            aggregatedMessage.setContent(aggregatedContents);
            aggregatedMessage.setMessageType(value.getMessageType());
            aggregatedMessage.setRequestTypeID(value.getRequestTypeID());
            aggregatedMessage.setParallelism(value.getParallelism());
            aggregatedMessage.setTimestamp(new Date());

            out.collect(aggregatedMessage);

            // Clear the state for this key so that the next request starts fresh.
            messagesState.clear();
        }
    }
}

