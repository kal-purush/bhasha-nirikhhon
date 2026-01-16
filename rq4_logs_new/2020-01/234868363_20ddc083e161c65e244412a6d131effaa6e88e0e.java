package com.example.bluetooth;

import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Parcelable;
import android.support.v7.app.AlertDialog;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MainActivity extends AppCompatActivity {
     private  Button scanbtn;
    private ListView scanlistView;
ArrayList<String> stringArrayList = new ArrayList<String>();
ArrayAdapter<String> arrayAdapter;
BluetoothAdapter myadapter=BluetoothAdapter.getDefaultAdapter();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
         scanbtn=(Button) findViewById(R.id.scanbtn);
        scanlistView=(ListView) findViewById(R.id.scanlistview);
scanbtn.setOnClickListener(new View.OnClickListener(){

    @Override
    public void onClick(View view) {
        myadapter.startDiscovery();
    }});
IntentFilter intentFilter = new IntentFilter(BluetoothDevice.ACTION_FOUND);
registerReceiver(myReceiver,intentFilter);
arrayAdapter= new ArrayAdapter<String>(getApplicationContext(),android.R.layout.simple_list_item_1,stringArrayList);

    scanlistView.setAdapter(arrayAdapter);}



BroadcastReceiver myReceiver = new BroadcastReceiver() {




    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();
        if (BluetoothDevice.ACTION_FOUND.equals(action)) {
            BluetoothDevice device = intent.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE);

           if(device.getName() != null) {
               stringArrayList.add(device.getName());

               arrayAdapter.notifyDataSetChanged();
           }
        }


    }};
}