package com.ramola.vibhav;

import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.View;

import java.util.ArrayList;
import java.util.Set;

public class BluetoothDeviceActivity extends AppCompatActivity {

    private RecyclerView recyclerView;
    private BluetoothDeviceAdapter adapter;
    private BluetoothAdapter bluetoothAdapter;
    private ArrayList<BluetoothDevice> list = new ArrayList<>();
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_bluetooth_device);
        recyclerView = (RecyclerView) findViewById(R.id.list);

        recyclerView.setLayoutManager(new LinearLayoutManager(this));
        adapter = new BluetoothDeviceAdapter();
        recyclerView.setAdapter(adapter);
        recyclerView.addOnItemTouchListener(new RecyclerItemClickListener(this, new RecyclerItemClickListener.OnItemClickListener() {
            @Override
            public void onItemClick(View view, int position) {
                Intent i =new Intent(BluetoothDeviceActivity.this,ClientThread.class);
                i.putExtra(ClientThread.ADDRESS,list.get(position).getAddress());
                startService(i);
                finish();
            }
        }));

        recyclerView.addItemDecoration(new DividerItemDecoration(this,LinearLayoutManager.VERTICAL));

        bluetoothAdapter = BluetoothAdapter.getDefaultAdapter();

        if(bluetoothAdapter!=null){


            for(BluetoothDevice device :bluetoothAdapter.getBondedDevices()){
                Log.d("name",device.getName());
                list.add(device);
            }
            adapter.refresh(list);
        }

    }
}