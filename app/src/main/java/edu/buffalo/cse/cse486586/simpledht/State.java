package edu.buffalo.cse.cse486586.simpledht;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents the state of this node
 *
 * Created by kishore on 3/18/16.
 */
public class State {

    private boolean joined; // set after joining the ring
    private String predNode;
    private String sucNode;

    public synchronized String getPredNode() {
        return predNode;
    }

    public synchronized void setPredNode(String predNode) {
        this.predNode = predNode;
    }

    public synchronized String getSucNode() {
        return sucNode;
    }

    public synchronized void setSucNode(String sucNode) {
        this.sucNode = sucNode;
    }

    public synchronized boolean isJoined() {
        return joined;
    }

    public synchronized void setJoined(boolean joined) {
        this.joined = joined;
    }
}
