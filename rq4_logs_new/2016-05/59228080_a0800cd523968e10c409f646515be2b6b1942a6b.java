package it.ivotek.poor2.client;


import java.util.ArrayList;
import java.util.List;

import android.bluetooth.BluetoothClass;
import android.bluetooth.BluetoothDevice;
import android.content.Context;

import it.ivotek.poor2.util.Bluetooth;

/**
 * Client per robot via bluetooth RFCOMM.
 * @author Daniele Ricci
 */
public class BluetoothRobotClient implements
        IRobotClient<BluetoothRobotConnectInfo>,
        Bluetooth.DiscoveryCallback, Bluetooth.CommunicationCallback {

    private final Context mContext;
    private final Bluetooth mBluetooth;
    private final List<RobotConnectInfo> mScanned;

    private RobotDiscoveryListener mDiscoverListener;
    private RobotConnectListener mConnectListener;
    private BluetoothRobotConnectInfo mRobot;

    public BluetoothRobotClient(Context context) {
        mContext = context;
        mBluetooth = new Bluetooth(context);
        mBluetooth.setDiscoveryCallback(this);
        mBluetooth.setCommunicationCallback(this);
        mScanned = new ArrayList<>();
    }

    @Override
    public void discover(RobotDiscoveryListener listener) {
        if (mDiscoverListener != null)
            throw new IllegalArgumentException("Another discovery is in progress");

        // imposta tutto e fai partire lo scan
        mDiscoverListener = listener;
        mScanned.clear();
        mBluetooth.scanDevices();
    }

    @Override
    public void cancelDiscovery() {
        mDiscoverListener = null;
    }

    @Override
    public void connect(BluetoothRobotConnectInfo robot, RobotConnectListener listener) {
        if (mConnectListener != null)
            throw new IllegalArgumentException("Another connection is active or in progress");

        mConnectListener = listener;
        mRobot = robot;
        mBluetooth.connectToAddress(robot.getAddress());
    }

    @Override
    public void disconnect() {
        if (mConnectListener != null) {
            mBluetooth.disconnect();
        }
    }

    @Override
    public void onFinish() {
        final RobotDiscoveryListener l = mDiscoverListener;
        if (l != null) {
            l.discovered(mScanned);
            mDiscoverListener = null;
            mScanned.clear();
        }
    }

    @Override
    public void onDevice(BluetoothDevice device) {
        // converti device in connectInfo e aggiungi alla lista
        String address = device.getAddress();
        String name = device.getName();
        BluetoothClass btClass = device.getBluetoothClass();
        mScanned.add(new BluetoothRobotConnectInfo(address, name, btClass));
    }

    @Override
    public void onPair(BluetoothDevice device) {
    }

    @Override
    public void onUnpair(BluetoothDevice device) {
    }

    @Override
    public void onConnect(BluetoothDevice device) {
        final RobotConnectListener l = mConnectListener;
        final BluetoothRobotConnectInfo robot = mRobot;
        if (l != null) {
            l.onConnected(robot);
        }
    }

    @Override
    public void onDisconnect(BluetoothDevice device, String message) {
        final RobotConnectListener l = mConnectListener;
        final BluetoothRobotConnectInfo robot = mRobot;
        if (l != null) {
            l.onDisconnected(robot);
            mConnectListener = null;
            mRobot = null;
        }
    }

    @Override
    public void onMessage(String message) {

    }

    @Override
    public void onError(String message) {
    }

    @Override
    public void onConnectError(BluetoothDevice device, String message) {

    }

}