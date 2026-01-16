package com.ramola.vibhav;

import android.app.IntentService;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothServerSocket;
import android.bluetooth.BluetoothSocket;
import android.content.Intent;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

/**
 * Created by user on 4/1/2017.
 */
public class ServerSide extends IntentService {
    public static final String SERVER_NAME="EasyOutPass";
    private static final UUID uuid
            =
            UUID.fromString("00001101-0000-1000-8000-00805F9B34FB");
    private BluetoothServerSocket bluetoothServerSocket;
    private BluetoothAdapter bluetoothAdapter;
    private BluetoothSocket bluetoothSocket;
    /**
     * Creates an IntentService.  Invoked by your subclass's constructor.
     *
     * @param name Used to name the worker thread, important only for debugging.
     */

    public ServerSide(String name) {
        super(name);
    }
    public ServerSide()
    {
        this(SERVER_NAME);
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        Log.d(SERVER_NAME,"Server Has Started");
     bluetoothAdapter=BluetoothAdapter.getDefaultAdapter();
        if(bluetoothAdapter!=null) {
            String line = null;
            while (true) {
                try {
                    bluetoothServerSocket = bluetoothAdapter.listenUsingRfcommWithServiceRecord("HC-05", uuid);
                    bluetoothSocket = bluetoothServerSocket.accept();
                    InputStream inputStream = bluetoothSocket.getInputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                    Log.d(SERVER_NAME, "Data Has Start Receiving");
                    StringBuilder out = new StringBuilder();
                    while ((line = reader.readLine()) != null) {
                        out.append(line);
                    }

                    Log.d(SERVER_NAME, line);   //Prints the string content read from input stream
                    reader.close();

                } catch (IOException e) {
                    e.printStackTrace();
                    Log.d(SERVER_NAME, "Some Error Occur");
                }

            }
        }
    }
}