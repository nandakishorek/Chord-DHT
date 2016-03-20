package edu.buffalo.cse.cse486586.simpledht;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by kishore on 3/18/16.
 */
public class Message {
    private static final String TAG = Message.class.getSimpleName();
    private static final String DELIM = "|";

    // message type join - join request, lookup - key lookup
    public enum Type {JOIN, LOOKUP, ADD, SUCC, PRED};

    private Type type;
    private String key; // query key
    private List<Map.Entry<String, String>> result = new ArrayList<Map.Entry<String, String>>(); // query result

    private String predNode;
    private String predNodeId;
    private String succNode;
    private String succNodeId;
    private String nodePort; // port of the JOIN requester
    private String nodeId; // hash of the node port

    public Message(Type type, String nodePort, String nodeId) {
        this.type = type;
        this.nodePort = nodePort;
        this.nodeId = nodeId;
    }

    public Message(Type type, String key) {
        this.type = type;
        this.key = key;
    }

    public Message(String msg) {
        parseMessage(msg);
    }

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

    public String getNodePort() {
        return nodePort;
    }

    public void setNodePort(String nodePort) {
        this.nodePort = nodePort;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getPredNodeId() {
        return predNodeId;
    }

    public void setPredNodeId(String predNodeId) {
        this.predNodeId = predNodeId;
    }

    public String getSuccNodeId() {
        return succNodeId;
    }

    public void setSuccNodeId(String succNodeId) {
        this.succNodeId = succNodeId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.type.toString() + DELIM);
        sb.append(this.key + DELIM);
        sb.append(this.nodePort + DELIM);
        sb.append(this.nodeId + DELIM);
        sb.append(this.predNode + DELIM);
        sb.append(this.predNodeId + DELIM);
        sb.append(this.succNode + DELIM);
        sb.append(this.succNodeId + DELIM);
        sb.append(this.result.size() + DELIM);
        Iterator<Map.Entry<String, String>> iter = result.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            sb.append(entry.getKey() + DELIM);
            sb.append(entry.getValue() + DELIM);
        }
        return sb.toString();
    }

    private void parseMessage(String rcvMessage) {
        String[] vals = rcvMessage.split("\\"+DELIM);
        this.type = Type.valueOf(vals[0]);
        this.key = vals[1];
        this.nodePort = vals[2];
        this.nodeId = vals[3];
        this.predNode = vals[4];
        this.predNodeId = vals[5];
        this.succNode = vals[6];
        this.succNodeId = vals[7];
        int resultSize = Integer.parseInt(vals[8]);
        for (int i = 0; i < resultSize; ++i) {
            Map.Entry<String, String> entry = new AbstractMap.SimpleEntry<String, String>(vals[i + 9], vals[i + 10]);
            result.add(entry);
        }
    }
}
