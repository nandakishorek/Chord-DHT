package edu.buffalo.cse.cse486586.simpledht;

import java.util.List;
import java.util.Map;

/**
 * Created by kishore on 3/18/16.
 */
public class Message {
    private static final String TAG = Message.class.getSimpleName();

    // message type join - join request, peers - neighbours, lookup - key lookup
    public enum Type {JOIN, PEERS, LOOKUP};

    private Type type;
    private String key; // query key
    private List<Map.Entry<String, String>> result; // query result

    private String predNode;
    private String succNode;
    private String nodeId;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<Map.Entry<String, String>> getResult() {
        return result;
    }

    public void setResult(List<Map.Entry<String, String>> result) {
        this.result = result;
    }

    public String getPredNode() {
        return predNode;
    }

    public void setPredNode(String predNode) {
        this.predNode = predNode;
    }

    public String getSuccNode() {
        return succNode;
    }

    public void setSuccNode(String succNode) {
        this.succNode = succNode;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
