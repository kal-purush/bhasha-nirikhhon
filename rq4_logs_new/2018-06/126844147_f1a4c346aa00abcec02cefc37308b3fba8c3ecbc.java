package com.example.a.myapplication.OBD.ObdData;

import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.content.Context;
import android.os.Handler;

import com.example.a.myapplication.OBD.ObdConfigration.ObdConfig;
import com.example.a.myapplication.OBD.obdApi.ObdCommand;
import com.example.a.myapplication.OBD.obdConnection.obdBlutoothManager;

import java.util.Set;

/**
 * class tho manage and start the connection of obd
 * use this to create object that is able to start the connection
 */


public class obdStart {

    private obdBlutoothManager mobdBlutoothManager = null;
    private BluetoothAdapter mBluetoothAdapter = null;
    private String mConnectedDeviceName;
    private boolean isRun = false;

    /**
     * embty constructor
     */

    public obdStart() {

    }

    /*
     * check if the connection was started
     * so not to start connection again
     *
     * */
    public boolean isRun() {
        return isRun;
    }

    /*
     * set up obd bluetooth manager
     * @param Context of the calling activity
     * @param Hamdler mhandler to be able to update the main thread UI
     * */

    public void setupobdManager(Context c, Handler mHandler) {


        // Initialize the array adapter for the conversation thread
        // mConversationArrayAdapter = new ArrayAdapter<String>(MainActivity.this, R.layout.message);
        // mConversationView.setAdapter(mConversationArrayAdapter);


        // Initialize the BluetoothChatService to perform bluetooth connections
        mobdBlutoothManager = new obdBlutoothManager(c, mHandler);
        isRun = true;
        // Initialize the buffer for outgoing messages
        //mOutStringBuffer = new StringBuffer("");

        //my eftkasat
        connectDevice();

        /****for testing only***/
        obdLiveData m = new obdLiveData(1);
        //int i =0;
        for (ObdCommand command : ObdConfig.getCommands())
        {
            m.addData(command.getName()+": Not connected");
            //i++;
        }

    }


    /**
     * Establish connection with other device
     */
    private void connectDevice(/*Intent data, boolean secure*/) {
        /*// Get the device MAC address
        String address = data.getExtras()
                .getString(DeviceListActivity.EXTRA_DEVICE_ADDRESS);
        // Get the BluetoothDevice object
        BluetoothDevice device = mBluetoothAdapter.getRemoteDevice(address);
        // Attempt to connect to the device
        mobdBlutoothManager.connect(device);
    */


        //my eftekasat

        // start listening as server
        //     mobdBlutoothManager.start();

        //get the paired devices
        Set<BluetoothDevice> myset = mobdBlutoothManager.findAllDevices();

        //connect to the desired device
        for (BluetoothDevice device : myset) {

            String Name = device.getName();

            //OBDII   Sony Xperia J  HUAWEI Y6III
            if (Name.equals("OBDII")) {
                mConnectedDeviceName = Name;
                mobdBlutoothManager.connect(device);
            }

        }


      /*  AcceptThread acceptThread = new AcceptThread();
        acceptThread.start();

        */


    }

}