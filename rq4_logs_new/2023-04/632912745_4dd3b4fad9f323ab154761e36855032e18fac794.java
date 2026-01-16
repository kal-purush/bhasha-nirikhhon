package com.example.socketchatting;

import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class ClientManagerThread extends Thread {

    private Socket socket;
    private String id;
    String TAG = "[ClientManagerThread Class]";

    @Override
    public void run() {
        super.run();
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String text;

            while (true) {
                text = in.readLine();
                Log.i(TAG, "text check : " + text);
                if (text != null) {
                    for (int i = 0; i < MyServer.outList.size(); ++i) {
                        MyServer.outList.get(i).println(text);
                        MyServer.outList.get(i).flush();
                        Log.i(TAG, "MyServer.outList.get(i) check : " + MyServer.outList.get(i));
                    } // for END
                } // if END
            } // while END

        } catch (IOException e) {
            e.printStackTrace();
        } // catch END
    } // run END

    public void setSocket(Socket socket) {
        this.socket = socket;
    } // setSocket END
} // CLASS END