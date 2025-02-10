package infore.SDE.messages;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Date;
import java.util.Map;

/**
 * A more sophisticated Message class that can carry:
 *   - A string or map for JSON-like data (content)
 * and is categorized by an enum MessageType (RESPONSE, LOG, ERROR).
 */
@JsonInclude(Include.NON_NULL)
public class Message extends SDEOutput {

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy HH:mm:ss", timezone = "UTC")
    private Date timestamp;
    private String key;
    private int parallelism;
    private String relatedRequestIdentifier;
    private MessageType messageType;

    // This can store either a String or a Map
    private Object content;

    // New member variable to hold request type ID
    private int requestTypeID;

    // ---- Constructors ----

    /**
     * Default constructor (needed for Jackson deserialization).
     */
    public Message() {
        this.timestamp = new Date();
    }

    /**
     * Constructor for text-based or map-based content.
     *
     * @param messageType the type of the message
     * @param content     the content (String or Map)
     */
    public Message(MessageType messageType, Object content) {
        this.timestamp = new Date();
        this.messageType = messageType;
        this.content = content;
    }

    /**
     * Constructor with related request identifier.
     *
     * @param messageType               the type of the message
     * @param content                   the content (String or Map)
     * @param relatedRequestIdentifier  the related request identifier
     */
    public Message(MessageType messageType, Object content, String relatedRequestIdentifier) {
        this.timestamp = new Date();
        this.messageType = messageType;
        this.content = content;
        this.relatedRequestIdentifier = relatedRequestIdentifier;
    }

    /**
     * Constructor with partial fields.
     *
     * @param messageType               the type of the message
     * @param content                   the content (String or Map)
     * @param relatedRequestIdentifier  the related request identifier
     * @param requestTypeID             the request type ID
     */
    public Message(MessageType messageType, Object content, String relatedRequestIdentifier, int requestTypeID) {
        this.timestamp = new Date();
        this.messageType = messageType;
        this.content = content;
        this.relatedRequestIdentifier = relatedRequestIdentifier;
        this.requestTypeID = requestTypeID;
    }

    /**
     * Constructor with all fields.
     *
     * @param messageType               the type of the message
     * @param content                   the content (String or Map)
     * @param relatedRequestIdentifier  the related request identifier
     * @param requestTypeID             the request type ID
     */
    public Message(MessageType messageType, Object content, String relatedRequestIdentifier, int requestTypeID, String key, int parallelism) {
        this.timestamp = new Date();
        this.messageType = messageType;
        this.content = content;
        this.relatedRequestIdentifier = relatedRequestIdentifier;
        this.requestTypeID = requestTypeID;
        this.key = key;
        this.parallelism = parallelism;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getRelatedRequestIdentifier() {
        return relatedRequestIdentifier;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public Object getContent() {
        return content;
    }

    public int getParallelism(){
        return parallelism;
    }

    public int getRequestTypeID() {
        return requestTypeID;
    }

    public void setRelatedRequestIdentifier(String relatedRequestIdentifier) {
        this.relatedRequestIdentifier = relatedRequestIdentifier;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public void setRequestTypeID(int requestTypeID) {
        this.requestTypeID = requestTypeID;
    }

    /**
     * Serializes this Message to a pretty JSON string.
     */
    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(this);
    }

    @Override
    public String toString() {
        if (content instanceof String) {
            return (String) content;
        } else if (content instanceof Map) {
            try {
                return new ObjectMapper().writeValueAsString(content);
            } catch (JsonProcessingException e) {
                return content.toString();
            }
        }
        return "Empty message";
    }

    @Override
    public int getNoOfP() {
        return parallelism;
    }
}
