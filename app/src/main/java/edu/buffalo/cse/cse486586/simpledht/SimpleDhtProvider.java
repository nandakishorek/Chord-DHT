package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.ConditionVariable;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = SimpleDhtProvider.class.getName();

    // column names
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String[] COLUMN_NAMES = {KEY_FIELD, VALUE_FIELD};

    static final int SERVER_PORT = 10000;

    private String mPort; // the port of this node
    private String mNodeId; // hash of the emulator port, ex. hash("5554");
    private ServerTask mServerTask;
    private State mState = new State();
    private Cursor mCursor; // will hold the query result from the successor
    private ConditionVariable mQueryDoneCV = new ConditionVariable(false);
    private int mDelCount;
    private ConditionVariable mDeleteDoneCV = new ConditionVariable(false);

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v(TAG, "delete key " + selection);
        if (mState.isJoined() && !"@".equals(selection) && ("*".equals(selection) || !isLocal(selection))) {
            String[] successor = mState.getSucNode();
            Message message = new Message(Message.Type.DEL, mPort, mNodeId);
            message.setKey(selection);

            if (selectionArgs == null) {
                // The starting node in the delete query chain
                // set the nodeId and nodePort in the message to current node
                message.setNodePort(mPort);
                message.setNodeId(mNodeId);
            } else if (successor[0].equals(selectionArgs[0])) {
                // last node in the chain
                Log.v(TAG, "Last node in the chain, delete local");
                mDelCount += deleteLocal(selection);
                return mDelCount;
            } else {
                // set the node port and node id to the origin
                message.setNodePort(selectionArgs[0]);
                message.setNodeId(selectionArgs[1]);
            }

            // set the successor node details
            message.setSuccNode(successor[0]);
            message.setSuccNodeId(successor[1]);

            new DeleteTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

            // wait for all the peers to delete
            mDeleteDoneCV.block();
            mDeleteDoneCV.close();

            Log.v(TAG, "Delete on peers finished count " + mDelCount);
            return mDelCount;
        } else {
            return deleteLocal(selection);
        }
    }

    private int deleteLocal(String key) {
        if (key.equals("*") || key.equals("@")) {
            // delete everything
            String[] allKeys = getContext().fileList();
            for (String k : allKeys) {
                getContext().deleteFile(k);
                Log.v(TAG, "delete local key " + k);
            }
            return allKeys.length;
        } else if (getContext().deleteFile(key)) {
            Log.v(TAG, "delete local key " + key);
            return 1;
        }
        Log.v(TAG, "delete local key " + key + " not found");
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        Log.v(TAG, "getType");
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = (String) values.get(KEY_FIELD);
        String val = (String) values.get(VALUE_FIELD);

        if (mState.isJoined()) {
            if (isLocal(key)) {
                insertLocal(key, val);
            } else {
                insertSuccessor(key, val);
            }
        } else {
            insertLocal(key, val);
        }
        return uri;
    }

    /**
     * Method to determine whether the key belongs to this node or not
     *
     * @param key
     * @return
     */
    private boolean isLocal(String key) {
        try {
            String keyHash = HashUtility.genHash(key);
            String[] pred = mState.getPredNode();
            if (pred[1].compareTo(mNodeId) > 0) {
                // 0 lies in between this and pred
                if (pred[1].compareTo(keyHash) < 0 || mNodeId.compareTo(keyHash) >= 0) {
                    return true;
                }
                return false;
            } else if (mNodeId.compareTo(keyHash) >= 0 && pred[1].compareTo(keyHash) < 0) {
                return true;
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "SHA-1 not supported");
            e.printStackTrace();
        }
        return false;
    }

    private void insertLocal(String key, String val) {
        try (FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE)) {
            fos.write(val.getBytes());
            fos.flush();
        } catch (FileNotFoundException fnf) {
            Log.e(TAG, "File not found - " + key);
        } catch (IOException ioe) {
            Log.e(TAG, "Error writing to file - " + key);
        }
        Log.v(TAG, "insert local key: " + key + " value: " + val);
    }

    /**
     * Pass the key, val to successor for insertion
     *
     * @param key
     * @param val
     */
    private void insertSuccessor(String key, String val) {
        String[] successor = mState.getSucNode();
        Message message = new Message(Message.Type.ADD, successor[0], successor[1]);
        message.getResult().add(new AbstractMap.SimpleEntry<String, String>(key, val));
        new InsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }

    @Override
    public boolean onCreate() {
        Log.v(TAG, "onCreate");

        // determine the port of this node
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        this.mPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.v(TAG,"SimpleDhtProvider port " + mPort);

        // gen node id
        try {
            mNodeId = HashUtility.genHash(String.valueOf(Integer.parseInt(mPort) / 2));
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "onCreate: SHA-1 not supported");
        }

        // start the server thread
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 5);
            mServerTask = new ServerTask(mPort, mNodeId, mState, getContext().getContentResolver());
            mServerTask.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket - " + e.getMessage());
        }

        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if (mState.isJoined() && !"@".equals(selection) && ("*".equals(selection) || !isLocal(selection))) {

            String[] successor = mState.getSucNode();
            Message message = new Message(Message.Type.LOOKUP, selection);

            if (selectionArgs == null) {
                // The starting node in the query chain
                // set the nodeId and nodePort in the message to current node
                message.setNodePort(mPort);
                message.setNodeId(mNodeId);
            } else if (successor[0].equals(selectionArgs[0])) {
                // last node in the chain
                Log.v(TAG, "Last node in the chain, return local");
                return queryLocal(selection);
            } else {
                // set the node port and node id to the origin
                message.setNodePort(selectionArgs[0]);
                message.setNodeId(selectionArgs[1]);
            }

            // set the successor node details
            message.setSuccNode(successor[0]);
            message.setSuccNodeId(successor[1]);

            // pass the query to successor
            new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

            // wait for the query to complete
            mQueryDoneCV.block();
            mQueryDoneCV.close(); // reset the state
            Log.v(TAG, "query from peers - done");

            return mCursor;
        } else {
            return queryLocal(selection);
        }
    }

    private Cursor queryLocal(String key) {
        Log.v(TAG, "queryLocal key " + key);
        MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
        if (key.equals("*") || key.equals("@")){
            String[] allKeys = getContext().fileList();
            for (String k : allKeys) {
                addValueToCursor(k, cursor);
            }
        } else {
            addValueToCursor(key, cursor);
        }
        return cursor;
    }

    private void addValueToCursor(String key, MatrixCursor cursor) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(getContext().openFileInput(key)))) {
            String value = br.readLine();
            cursor.addRow(new String[]{key, value});
        } catch (IOException ioe) {
            Log.v(TAG, "query local key - " + key + "not found");
        }
    }

    /**
     * Adds all key, val in local partition to the list
     *
     * @param result list to be populated
     */
    private void addAllLocal(List<Map.Entry<String, String>> result) {
        if (result != null) {
            String[] allKeys = getContext().fileList();
            for (String key : allKeys) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(getContext().openFileInput(key)))) {
                    String value = br.readLine();
                    result.add(new AbstractMap.SimpleEntry<String, String>(key, value));
                } catch (IOException ioe) {
                    Log.v(TAG, "addAllLocal key - " + key + "not found");
                }
            }
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        Log.v(TAG, "update");
        return 0;
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    /**
     * AsyncTask to pass the insert message to successor
     */
    private class InsertTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(message.getNodePort()));
                 BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            ){
                String msgToSend = message.toString();
                bw.write(msgToSend + "\n");
                bw.flush();
                Log.v(TAG, "sent insert to successor " + msgToSend);
            } catch (IOException ioe) {
                Log.e(TAG, "Error sending insert to successor");
                ioe.printStackTrace();
            }

            return null;
        }
    }

    /**
     * AsyncTask to pass the query message to successor
     */
    private class QueryTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(message.getSuccNode()));
                 BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                 BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            ){
                // pass the query to successor
                String msgToSend = message.toString();
                bw.write(msgToSend + "\n");
                bw.flush();
                Log.v(TAG, "passed query to successor " + msgToSend);

                // read the response back
                String line = br.readLine();
                Message respMsg = new Message(line);
                List<Map.Entry<String, String>> queryResult = respMsg.getResult();

                // if query if "*", then add local results
                if ("*".equals(message.getKey())) {
                    addAllLocal(queryResult);
                }

                Log.v(TAG, "query result - " + queryResult.toString());

                if (queryResult.size() == 0) {
                    Log.v(TAG, "query key " + message.getKey() + " not found");
                    mCursor =  null;
                } else {
                    MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
                    for (Map.Entry<String, String> entry : queryResult) {
                        cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                    }
                    mCursor = cursor;
                }
            } catch (IOException ioe) {
                Log.e(TAG, "Error sending query to successor");
                ioe.printStackTrace();
            }

            // notify the main thread
            mQueryDoneCV.open();
            Log.v(TAG, "notified the main thread");

            return null;
        }
    }

    /**
     * AsyncTask to pass the delete message to successor
     */
    private class DeleteTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            try (Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(message.getSuccNode()));
                 BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                 BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            ){
                // pass the query to successor
                String msgToSend = message.toString();
                bw.write(msgToSend + "\n");
                bw.flush();
                Log.v(TAG, "passed delete to successor " + msgToSend);

                // read the response back
                String line = br.readLine();
                Message respMsg = new Message(line);
                mDelCount += Integer.parseInt(respMsg.getKey());
                Log.v(TAG, "delete response from peer - " + line);

            } catch (IOException ioe) {
                Log.e(TAG, "Error sending query to successor");
                ioe.printStackTrace();
            }

            // notify the main thread
            mDeleteDoneCV.open();
            Log.v(TAG, "notified the main thread");

            return null;
        }
    }
}
