package com.boring.cvlabbluetoothtest;

import android.Manifest;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.pm.PackageManager;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Set;

public class BTDevice {

    // 내 멋대로 만든 API임 알아서들 쓰세요

    private AppCompatActivity activity;
    private BluetoothAdapter BTAdapter;

    // 생성자
    public BTDevice(AppCompatActivity appCompatActivity) {
        this.activity = appCompatActivity;
        requestPermissions();
    }
    private void requestPermissions() {
        // 권한 잔뜩주기
        if (ContextCompat.checkSelfPermission(activity.getBaseContext(), Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED
                || ContextCompat.checkSelfPermission(activity.getBaseContext(), Manifest.permission.BLUETOOTH_ADMIN) != PackageManager.PERMISSION_GRANTED) {

            ActivityCompat.requestPermissions(activity, new String[]{Manifest.permission.ACCESS_FINE_LOCATION,
                    Manifest.permission.BLUETOOTH_ADMIN}, 100);
            // 재시작하기
            activity.finish();
            activity.startActivity(new Intent(activity, activity.getClass()));
        } else {
            // 기본 받아오기
            BTAdapter = BluetoothAdapter.getDefaultAdapter();
        }
    }
    // 블루투스 스위치 on
    public void on(){
        if (!BTAdapter.isEnabled()) {
            Intent turnOn = new Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE);
            activity.startActivityForResult(turnOn, 0);
            Toast.makeText(activity.getApplicationContext(), "스위치 on", Toast.LENGTH_SHORT).show();
        } else {
            Toast.makeText(activity.getApplicationContext(), "스위치가 이미 켜져있음", Toast.LENGTH_SHORT).show();
        }
    }
    // 블루투스 스위치 off
    public void off(){
        BTAdapter.disable();
        Toast.makeText(activity.getApplicationContext(), "스위치 off" , Toast.LENGTH_SHORT).show();
    }

    // 주변 장치 찾기
    // 리스트뷰에 담아버리기
    public void findDevice(Context context, ListView listView) {
        final ArrayAdapter<String> newDevice = new ArrayAdapter<>(context, android.R.layout.simple_list_item_1);
        IntentFilter filter = new IntentFilter(BluetoothDevice.ACTION_FOUND);
        BroadcastReceiver receiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                String action = intent.getAction();
                if (BluetoothDevice.ACTION_FOUND.equals(action)) {
                    final BluetoothDevice device = intent.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE);
                    newDevice.add(device.getName() + " " + device.getAddress());
                }
            }
        };
        context.registerReceiver(receiver, filter);
        BTAdapter.startDiscovery();
        listView.setAdapter(newDevice);
    }
    public void pairing(BluetoothDevice device, BluetoothAdapter adapter, String address) {
        device = adapter.getRemoteDevice(address);
        try {
            Method method = device.getClass().getMethod("createBond",(Class[]) null);
            method.invoke(device, (Object[])null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 페어링된 기기를 리스트뷰에 넣기
    public void paireddeviceList(ListView lv){
        ArrayList<String> deviceList = new ArrayList<>();
        Set<BluetoothDevice> pairedDevices = BTAdapter.getBondedDevices();

        if (pairedDevices.size() < 1) {
            Toast.makeText(activity.getApplicationContext(), "No paired devices found", Toast.LENGTH_SHORT).show();
        } else {
            for (BluetoothDevice bt : pairedDevices)
                deviceList.add(bt.getName() + " " + bt.getAddress());
            Toast.makeText(activity.getApplicationContext(), "Showing paired devices", Toast.LENGTH_SHORT).show();
            final ArrayAdapter<String> adapter = new ArrayAdapter<String>(activity, android.R.layout.simple_list_item_1, deviceList);
            lv.setAdapter(adapter);

        }
    }
    // 두개의 리스너에 리스트뷰랑 텍스트뷰 둘다 등록하니
    // 만약 주소값을 얻고 싶으면 textView에서 getText()하자
    // 주변 기기 연결 리스너 등록
    public void connectListener(final BluetoothAdapter adapter, ListView listView, final TextView textView) {
        final String[] address = new String[1];
        listView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                String info = ((TextView) view).getText().toString();
                address[0] = info.substring(info.length() - 17);

                BluetoothDevice mDevice = adapter.getRemoteDevice(address[0]);
                try {
                    Method method = mDevice.getClass().getMethod("createBond",(Class[]) null);
                    method.invoke(mDevice, (Object[])null);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                textView.setText(address[0]);
            }
        });
    }
    // 페어링된 기기 리스너 등록
    public void pairedListener(final BluetoothAdapter adapter, ListView listView, final TextView textView) {
        final String[] address = new String[1];
        listView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                String info = ((TextView) view).getText().toString();
                address[0] = info.substring(info.length() - 17);
                textView.setText(address[0]);
            }
        });
    }
}