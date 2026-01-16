package com.htw.bluelock;

import java.io.IOException;
import java.util.UUID;

import android.app.Activity;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothSocket;
import android.content.Intent;
import android.os.Bundle;
import android.os.ParcelUuid;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.Toast;

public class BluelockMainScreen extends Activity {

	BluetoothDevice device;
	boolean connected = true;
	Button connect;
	Button shutdown;
	Button login;
	Button logout;

	protected void onResume() {
		super.onResume();
		Bundle b = (Bundle) this.getIntent().getBundleExtra("device");
		if (b != null) {
			device = b.getParcelable("device");
			Toast.makeText(this, "" + device.getAddress(), Toast.LENGTH_LONG).show();
			device.fetchUuidsWithSdp();
			
			try {
			BluetoothSocket socket = device.createInsecureRfcommSocketToServiceRecord(UUID.fromString("00001124-0000-1000-8000-00805f9b34fb"));
			socket.connect();
			socket.getOutputStream().write(new String("1").getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

		}
	}

	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_main);
		
		connect = (Button) findViewById(R.id.btnconnect);
		connect.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				Intent intent = new Intent(BluelockMainScreen.this, DeviceScanActivity.class);
				startActivity(intent);
			};
		});

		login = (Button) findViewById(R.id.btnLogin);
		login.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
			
			};
		});

		logout = (Button) findViewById(R.id.btnLogout);
		logout.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				
			};
		});

		shutdown = (Button) findViewById(R.id.btnShutdown);
		shutdown.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
	
			};
		});
		

	}
}