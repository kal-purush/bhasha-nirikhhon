package networking;

import android.util.Log;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;

public class Client {

    private String serverMessage;
    public static final String SERVERIP = "111.111.0.111"; //your computer IP address
    public static final int SERVERPORT = 8080;
    private OnMessageReceived mMessageListener = null;
    private boolean mRun = false;

    PrintWriter out;
    BufferedReader in;

    public Client(OnMessageReceived listener) {
        mMessageListener = listener;
    }

    public void sendMessage(String message){
        if (out != null && !out.checkError()) {
            out.println(message);
            out.flush();
        }
    }

    public void stopClient(){
        mRun = false;
    }

    public void run() {
        mRun = true;

        try {
            InetAddress serverAddr = InetAddress.getByName(SERVERIP);
            Log.e("Client", "Connecting...");

            //create a socket to make the connection with the server
            Socket socket = new Socket(serverAddr, SERVERPORT);

            try {
                //send the message to the server
                out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);

                Log.e("Client", "Sent.");
                Log.e("Client", "Done.");

                //receive the message which the server sends back
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                //in this while the client listens for the messages sent by the server
                while (mRun) {
                    serverMessage = in.readLine();

                    if (serverMessage != null && mMessageListener != null) {
                        //call the method messageReceived from MyActivity class
                        mMessageListener.messageReceived(serverMessage);
                    }
                    serverMessage = null;
                }

                Log.e("RESPONSE FROM SERVER", "Received Message: '" + serverMessage + "'");

            } catch (Exception e) {

                Log.e("TCP", "Error", e);

            } finally {

                socket.close();
            }

        } catch (Exception e) {

            Log.e("TCP", "Error", e);
        }
    }

    public interface OnMessageReceived {
        public void messageReceived(String message);
    }
}