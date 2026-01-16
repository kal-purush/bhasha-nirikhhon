package com.quarto;

import android.app.Activity;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothSocket;
import android.content.ComponentName;
import android.content.Intent;
import android.provider.Settings;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentTransaction;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.AppCompatImageView;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import java.util.Set;
import java.util.UUID;

import app.akexorcist.bluetotohspp.library.BluetoothSPP;
import app.akexorcist.bluetotohspp.library.BluetoothState;
import app.akexorcist.bluetotohspp.library.DeviceList;

public class MainActivity extends AppCompatActivity {

    Button btn_send, btn_conn;
    EditText edt_msg;
    TextView txt_msg;
    AppCompatImageView img_room_0_0, img_room_1_0, img_room_1_1, img_room_2_0, img_room_2_1, img_room_2_2
            , img_room_3_0, img_room_3_1, img_room_3_2, img_room_3_3, img_room_4_0, img_room_4_1, img_room_4_2
            , img_room_5_0, img_room_5_1, img_room_6_0;
    int quarto_drawables[] = {
            R.drawable.ic_circle_black_big_filled,
            R.drawable.ic_circle_black_big_hollow,
            R.drawable.ic_circle_black_small_filled,
            R.drawable.ic_circle_black_small_hollow,
            R.drawable.ic_circle_white_big_filled,
            R.drawable.ic_circle_white_big_hollow,
            R.drawable.ic_circle_white_small_filled,
            R.drawable.ic_circle_white_small_hollow,
            R.drawable.ic_square_black_big_filled,
            R.drawable.ic_square_black_big_hollow,
            R.drawable.ic_square_black_small_filled,
            R.drawable.ic_square_black_small_hollow,
            R.drawable.ic_square_white_big_filled,
            R.drawable.ic_square_white_big_hollow,
            R.drawable.ic_square_white_small_filled,
            R.drawable.ic_square_white_small_hollow
    };
    BluetoothSPP bt;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        create_layout();

        img_room_0_0.setImageResource(quarto_drawables[0]);
        img_room_1_0.setImageResource(quarto_drawables[1]);
        img_room_1_1.setImageResource(quarto_drawables[2]);
        img_room_2_0.setImageResource(quarto_drawables[3]);
        img_room_2_1.setImageResource(quarto_drawables[4]);
        img_room_2_2.setImageResource(quarto_drawables[5]);
        img_room_3_0.setImageResource(quarto_drawables[6]);
        img_room_3_1.setImageResource(quarto_drawables[7]);
        img_room_3_2.setImageResource(quarto_drawables[8]);
        img_room_3_3.setImageResource(quarto_drawables[9]);
        img_room_4_0.setImageResource(quarto_drawables[10]);
        img_room_4_1.setImageResource(quarto_drawables[11]);
        img_room_4_2.setImageResource(quarto_drawables[12]);
        img_room_5_0.setImageResource(quarto_drawables[13]);
        img_room_5_1.setImageResource(quarto_drawables[14]);
        img_room_6_0.setImageResource(quarto_drawables[15]);


        bt = new BluetoothSPP(getBaseContext());
        if(!bt.isBluetoothAvailable()) {
            Toast.makeText(getApplicationContext(), "Bluetooth is not available", Toast.LENGTH_SHORT).show();
            finish();
        }

        ConnectFragment connectFragment = new ConnectFragment();
        connectFragment.bt = bt;
        FragmentManager fragmentManager0 = getSupportFragmentManager();
        FragmentTransaction fragmentTransaction0 = fragmentManager0.beginTransaction();
        fragmentTransaction0.add(R.id.activity_main, connectFragment);
        fragmentTransaction0.addToBackStack("connectFragment");
        fragmentTransaction0.commit();

        btn_conn.setOnClickListener(new View.OnClickListener(){
            public void onClick(View v){
                if(bt.getServiceState() == BluetoothState.STATE_CONNECTED) {
                    bt.disconnect();
                } else {
                    if (bt.isServiceAvailable()) {
                        Intent intent = new Intent(getApplicationContext(), DeviceList.class);
                        startActivityForResult(intent, BluetoothState.REQUEST_CONNECT_DEVICE);
                    }else {
                        Intent intent = new Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE);
                        startActivityForResult(intent, BluetoothState.REQUEST_ENABLE_BT);
                    }
                }
            }
        });
        btn_send.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                bt.send(edt_msg.getText().toString(), true);
            }
        });

        bt.setOnDataReceivedListener(new BluetoothSPP.OnDataReceivedListener() {
            public void onDataReceived(byte[] data, String message) {
                txt_msg.setText(message);
            }
        });

        bt.setBluetoothConnectionListener(new BluetoothSPP.BluetoothConnectionListener() {
            public void onDeviceConnected(String name, String address) {
                Toast.makeText(getApplicationContext(), "به "+name+" متصل شدید.", Toast.LENGTH_SHORT).show();
            }
            public void onDeviceDisconnected() {
                Toast.makeText(getApplicationContext(), "ارتباط قطع شد.", Toast.LENGTH_SHORT).show();
            }
            public void onDeviceConnectionFailed() {
                Toast.makeText(getApplicationContext(), "اتصال برقرار نشد.", Toast.LENGTH_SHORT).show();
            }
        });

    }

    public void create_layout() {

        img_room_0_0 = (AppCompatImageView) findViewById(R.id.img_room_0_0);
        img_room_1_0 = (AppCompatImageView) findViewById(R.id.img_room_1_0);
        img_room_1_1 = (AppCompatImageView) findViewById(R.id.img_room_1_1);
        img_room_2_0 = (AppCompatImageView) findViewById(R.id.img_room_2_0);
        img_room_2_1 = (AppCompatImageView) findViewById(R.id.img_room_2_1);
        img_room_2_2 = (AppCompatImageView) findViewById(R.id.img_room_2_2);
        img_room_3_0 = (AppCompatImageView) findViewById(R.id.img_room_3_0);
        img_room_3_1 = (AppCompatImageView) findViewById(R.id.img_room_3_1);
        img_room_3_2 = (AppCompatImageView) findViewById(R.id.img_room_3_2);
        img_room_3_3 = (AppCompatImageView) findViewById(R.id.img_room_3_3);
        img_room_4_0 = (AppCompatImageView) findViewById(R.id.img_room_4_0);
        img_room_4_1 = (AppCompatImageView) findViewById(R.id.img_room_4_1);
        img_room_4_2 = (AppCompatImageView) findViewById(R.id.img_room_4_2);
        img_room_5_0 = (AppCompatImageView) findViewById(R.id.img_room_5_0);
        img_room_5_1 = (AppCompatImageView) findViewById(R.id.img_room_5_1);
        img_room_6_0 = (AppCompatImageView) findViewById(R.id.img_room_6_0);

        btn_send = (Button) findViewById(R.id.btn_send);
        btn_conn = (Button) findViewById(R.id.btn_conn);
        edt_msg = (EditText) findViewById(R.id.edt_msg);
        txt_msg = (TextView) findViewById(R.id.txt_msg);

    }

    public void onActivityResult(int requestCode, int resultCode, Intent data) {
        if (requestCode == BluetoothState.REQUEST_CONNECT_DEVICE) {
            if (resultCode == Activity.RESULT_OK) {
                bt.connect(data);
                btn_send.setClickable(true);
            }
        } else if (requestCode == BluetoothState.REQUEST_ENABLE_BT) {
            if (resultCode == Activity.RESULT_OK) {
                bt.setupService();
                bt.startService(BluetoothState.DEVICE_ANDROID);
                btn_send.setClickable(true);
            } else {
                Toast.makeText(getApplicationContext(), "Bluetooth was not enabled.", Toast.LENGTH_SHORT).show();
                btn_send.setClickable(false);
            }
        }
    }

    public void onStart() {
        super.onStart();
        if (bt==null) {
            bt = new BluetoothSPP(getBaseContext());
        }
        if(bt.isBluetoothEnabled()) {
            if(!bt.isServiceAvailable()) {
                bt.setupService();
                bt.startService(BluetoothState.DEVICE_ANDROID);
                btn_send.setClickable(true);
            }
        } else {
            Intent intent = new Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE);
            startActivityForResult(intent, BluetoothState.REQUEST_ENABLE_BT);
        }
    }
    public void onDestroy() {
        super.onDestroy();
        bt.stopService();
    }

}