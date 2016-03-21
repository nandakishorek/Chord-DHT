package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by kishore on 3/18/16.
 */
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

    private static final String TAG = ServerTask.class.getSimpleName();
    private static final String LEADER_PORT = "11108";

    private String mPort; // the port of this node
    private String mNodeId; // hash of node port
    private State mState;
    private TreeMap<String, String> mNodeMap;
    private MessageStore mMessageStore;

    public ServerTask(String myPort, String nodeId, State state, ContentResolver cr) {
        this.mPort = myPort;
        this.mNodeId = nodeId;
        this.mState = state;
        this.mNodeMap = new TreeMap<String, String>();
        if (mPort.equals(LEADER_PORT)) {
            // leader node
            mNodeMap.put(mNodeId, mPort);
        }
        this.mMessageStore = new MessageStore(cr);
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        Log.v(TAG, "ServerTask started");

        // try to join the ring
        if (!mPort.equals(LEADER_PORT)) {
            try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(LEADER_PORT));
                 BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            ) {
                // send join request
                Message msg = new Message(Message.Type.JOIN, mPort, mNodeId);
                String line = msg.toString();
                bw.write(line + "\n");
                bw.flush();
                Log.v(TAG, "sent join request - " + line);

                // receive join response
                line = br.readLine();
                if (line != null) {
                    msg = new Message(line);
                    handle_join_resp(msg);
                    Log.v(TAG, "Ring join successful");
                }
            } catch (IOException ioe) {
                Log.e(TAG, "Error joining the ring");
                ioe.printStackTrace();
            }
        }

        ServerSocket serverSocket = sockets[0];
        while (!isCancelled()) {
            try {
                Socket clientSocket = serverSocket.accept();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                     BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
                ) {
                    String line = br.readLine();
                    Log.v(TAG, "received message " + line);
                    Message msg = new Message(line);

                    switch (msg.getType()) {
                        case JOIN:
                            handle_join(msg, bw);
                            break;
                        case LOOKUP:
                            handle_lookup(msg, bw);
                            break;
                        case ADD:
                            Map.Entry<String, String> entry = msg.getResult().get(0);
                            mMessageStore.insert(entry.getKey(), entry.getValue());
                            break;
                        case DEL:
                            handle_delete(msg, bw);
                            break;
                        case SUCC:
                            mState.setSucNode(msg.getNodePort(), msg.getNodeId());
                            Log.v(TAG, "Changed successor to " + msg.getNodePort());
                            break;
                        case PRED:
                            mState.setPredNode(msg.getNodePort(), msg.getNodeId());
                            Log.v(TAG, "Changed predecessor to " + msg.getNodePort());
                            break;
                        default:
                            Log.e(TAG, "invalid message " + msg);
                            break;
                    }
                } catch (IOException ioe) {
                    Log.e(TAG, "Error writing or reading to client socket");
                    ioe.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "Error while accepting the client connection");
            }
        }

        try {
            serverSocket.close();
        } catch (IOException ioe) {
            Log.e(TAG, "Could not close server socket");
        }

        return null;
    }

    /**
     * Method to handle JOIN requests from peers
     * NOTE: only leader executes this
     * @param msg JOIN request
     * @param bw write handle to peer socket
     */
    private void handle_join(Message msg, BufferedWriter bw) {
        // create response message
        if (mNodeMap.size() == 1) {
            // first node join
            msg.setSuccNode(mPort);
            msg.setSuccNodeId(mNodeId);
            msg.setPredNode(mPort);
            msg.setPredNodeId(mNodeId);

            // add the new node to the table
            mNodeMap.put(msg.getNodeId(), msg.getNodePort());

            // set successor and predecessor for self
            mState.setPredNode(msg.getNodePort(), msg.getNodeId());
            mState.setSucNode(msg.getNodePort(), msg.getNodeId());
            mState.setJoined(true);
        } else {
            // find the successor
            Map.Entry<String, String> successor = mNodeMap.higherEntry(msg.getNodeId());
            if (successor == null) {
                successor = mNodeMap.firstEntry();
            }

            // find the predecessor
            Map.Entry<String, String> predecessor = mNodeMap.lowerEntry(msg.getNodeId());
            if (predecessor == null) {
                predecessor = mNodeMap.lastEntry();
            }

            // add the new node to the table
            mNodeMap.put(msg.getNodeId(), msg.getNodePort());

            msg.setSuccNode(successor.getValue());
            msg.setSuccNodeId(successor.getKey());
            msg.setPredNode(predecessor.getValue());
            msg.setPredNodeId(predecessor.getKey());
        }

        try {
            String resp = msg.toString();
            bw.write(resp + "\n");
            bw.flush();
            Log.v(TAG, "join response - " + resp);
        } catch (IOException e) {
            Log.e(TAG, "Error sending JOIN response");
            e.printStackTrace();
        }
    }

    /**
     * Method to process JOIN response from leader
     *
     * @param msg JOIN response message
     */
    private void handle_join_resp(Message msg) {
        // set predecessor and successor
        String succ = msg.getSuccNode();
        String pred = msg.getPredNode();
        mState.setSucNode(succ, msg.getSuccNodeId());
        mState.setPredNode(pred, msg.getPredNodeId());
        mState.setJoined(true);
        Log.v(TAG, "JOINED successor - " + succ + ", predecessor - " + pred);

        // send message to successor for updation
        msg.setType(Message.Type.PRED);
        msg.setNodePort(mPort);
        msg.setNodeId(mNodeId);
        try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(succ));
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        ) {
            bw.write(msg.toString() + "\n");
            bw.flush();
        } catch (IOException ioe) {
            Log.e(TAG, "Error sending message to successor");
            ioe.printStackTrace();
        }

        // send message to predecessor for updation
        msg.setType(Message.Type.SUCC);
        try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(pred));
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        ) {
            bw.write(msg.toString() + "\n");
            bw.flush();
        } catch (IOException ioe) {
            Log.e(TAG, "Error sending message to successor");
            ioe.printStackTrace();
        }
    }

    /**
     * Method to handle key lookup from peers
     *
     * @param msg Lookup message
     * @param bw write handle to peer socket
     */
    private void handle_lookup(Message msg, BufferedWriter bw) {
        // query local provider
        List<Map.Entry<String, String>> resultList = mMessageStore.query(msg.getKey(), msg.getNodePort(), msg.getNodeId());

        // send the results back to the origin
        msg.getResult().addAll(resultList);
        try{
            String resp = msg.toString();
            bw.write(resp + "\n");
            bw.flush();
            Log.v(TAG, "handle_lookup sent resp - " + resp);
        } catch (IOException ioe) {
            Log.v(TAG, "error sending back query response");
            ioe.printStackTrace();
        }
    }

    /**
     * Method to handle delete cmd from peers
     * @param msg
     * @param bw
     */
    private void handle_delete(Message msg, BufferedWriter bw) {
        // issue delete to local provider
        int delCount = mMessageStore.delete(msg.getKey(), msg.getNodePort(), msg.getNodeId());

        // send the response back
        msg.setKey(Integer.toString(delCount)); // send the number of keys deleted in the key field
        try{
            String resp = msg.toString();
            bw.write(resp + "\n");
            bw.flush();
            Log.v(TAG, "handle_delete sent resp - " + resp);
        } catch (IOException ioe) {
            Log.v(TAG, "error sending back delete response");
            ioe.printStackTrace();
        }
    }
}
