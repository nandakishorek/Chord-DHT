package edu.buffalo.cse.cse486586.simpledht;

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by kishore on 3/18/16.
 */
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

    private static final String TAG = ServerTask.class.getSimpleName();

    private String mPort; // the port of this node
    private State mState;

    public ServerTask(String myPort, State state) {
        this.mPort = myPort;
        this.mState = state;
    }

    @Override
    protected Void doInBackground(ServerSocket... sockets) {

        Log.v(TAG, "ServerTask started");
        ServerSocket serverSocket = sockets[0];

        while (!isCancelled()) {
            try {
                Socket clientSocket = serverSocket.accept();

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
}
