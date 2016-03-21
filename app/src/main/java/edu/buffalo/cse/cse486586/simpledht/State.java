package edu.buffalo.cse.cse486586.simpledht;

/**
 * Represents the state of this node
 *
 * Created by kishore on 3/18/16.
 */
public class State {

    private boolean joined; // set after joining the ring
    private String predNode;
    private String predNodeId;
    private String sucNode;
    private String sucNodeId;

    public synchronized String[] getPredNode() {
        String[] ret = new String[2];
        ret[0] = predNode;
        ret[1] = predNodeId;
        return ret;
    }

    public synchronized void setPredNode(String predNode, String predNodeId) {
        this.predNode = predNode;
        this.predNodeId = predNodeId;
    }

    public synchronized String[] getSucNode() {
        String[] ret = new String[2];
        ret[0] = sucNode;
        ret[1] = sucNodeId;
        return ret;
    }

    public synchronized void setSucNode(String sucNode, String sucNodeId) {
        this.sucNode = sucNode;
        this.sucNodeId = sucNodeId;
    }

    public synchronized boolean isJoined() {
        return joined;
    }

    public synchronized void setJoined(boolean joined) {
        this.joined = joined;
    }
}
