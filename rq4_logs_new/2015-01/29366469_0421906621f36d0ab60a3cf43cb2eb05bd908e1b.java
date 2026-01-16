package com.htw.bluelock;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothSocket;

public class BluetoothConnection {
	
	public static final String BLUELOCK_UUID = "00001124-0000-1000-8000-00805f9b34fb";
	public static HashMap<String,String> protocol = new HashMap<String,String>();
	
	static{
		protocol = new HashMap<String,String>();
		protocol.put("LOCK", "1");
		protocol.put("UNLOCK", "2");
		protocol.put("SHUTDOWN", "3");
		protocol.put("SHUTDOWN_TIME", "4;23");
	}
	
	private BluetoothSocket socket;
	private BluetoothDevice device;
	
	public BluetoothConnection(BluetoothDevice device){
		this.device = device;
	}
	
	public void connect() throws IOException {
		socket = device.createInsecureRfcommSocketToServiceRecord(UUID.fromString(BluetoothConnection.BLUELOCK_UUID));
		socket.connect();
	}
	
	public String write(String code) {
		try {
			if (socket.isConnected()) {
				socket.getOutputStream().write(code.getBytes());
			} else {
				socket.connect();
				socket.getOutputStream().write(code.getBytes());
			}
		} catch (IOException e) {
			e.printStackTrace();
			return "Couldn't send command to device, try to reconnect!";
		}
		return "Send command to device";
	}
}