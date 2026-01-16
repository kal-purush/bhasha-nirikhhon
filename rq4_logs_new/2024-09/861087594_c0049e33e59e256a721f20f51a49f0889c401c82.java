package com.raghav.ethglobalsingapore2024.javaapp;


import android.bluetooth.BluetoothAdapter;
import android.bluetooth.le.AdvertiseCallback;
import android.bluetooth.le.AdvertiseData;
import android.bluetooth.le.AdvertiseSettings;
import android.bluetooth.le.BluetoothLeAdvertiser;
import android.content.Context;
import android.content.pm.PackageManager;
import android.os.ParcelUuid;
import android.util.Log;
import android.view.View;
import android.widget.Toast;

import androidx.core.app.ActivityCompat;


public class BleAdvertiser {

    private BluetoothLeAdvertiser bluetoothLeAdvertiser;
    private AdvertiseCallback advertiseCallback;
    private Context context;
    public BleAdvertiser(Context context, BluetoothAdapter bluetoothAdapter) {
        this.bluetoothLeAdvertiser = bluetoothAdapter.getBluetoothLeAdvertiser();
        this.context = context;
        setupAdvertiseCallback();
    }



    private void setupAdvertiseCallback() {
        advertiseCallback = new AdvertiseCallback() {
            @Override
            public void onStartSuccess(AdvertiseSettings settingsInEffect) {
                Log.d("BLE", "Advertising started successfully");
            }

            @Override
            public void onStartFailure(int errorCode) {
                Log.e("BLE", "Advertising failed: " + errorCode);
            }
        };
    }

    public void startAdvertising(String message) {
        if (bluetoothLeAdvertiser == null) {
            Log.e("BLE", "BLE Advertising not supported");
            return;
        }

        byte[] messageBytes = message.getBytes();

        AdvertiseSettings advertiseSettings = new AdvertiseSettings.Builder()
                .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_LOW_LATENCY)
                .setTxPowerLevel(AdvertiseSettings.ADVERTISE_TX_POWER_HIGH)
                .setConnectable(false)
                .build();

        AdvertiseData advertiseData = new AdvertiseData.Builder()
                .addServiceData(ParcelUuid.fromString("0000180D-0000-1000-8000-00805F9B34FB"), messageBytes)
                .build();

        if (ActivityCompat.checkSelfPermission(context, android.Manifest.permission.BLUETOOTH_ADVERTISE) != PackageManager.PERMISSION_GRANTED) {
            Toast.makeText(context,
                    ActivityCompat.checkSelfPermission(context, android.Manifest.permission.BLUETOOTH_ADVERTISE)
                    , Toast.LENGTH_SHORT).show();

            bluetoothLeAdvertiser.startAdvertising(advertiseSettings, advertiseData, advertiseCallback);
        }
    }
        public void stopAdvertising () {
            if (ActivityCompat.checkSelfPermission(context, android.Manifest.permission.BLUETOOTH_ADVERTISE) != PackageManager.PERMISSION_GRANTED) {
                Toast.makeText(context,
                        ActivityCompat.checkSelfPermission(context, android.Manifest.permission.BLUETOOTH_ADVERTISE)
                        , Toast.LENGTH_SHORT).show();

                bluetoothLeAdvertiser.stopAdvertising(advertiseCallback);


                Log.d("BLE", "Advertising stopped");
            }
        }
    }