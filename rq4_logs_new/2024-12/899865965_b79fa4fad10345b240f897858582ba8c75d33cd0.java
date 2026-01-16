package com.mvc.sampling_machine_mobile_testing;
import android.annotation.SuppressLint;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.core.content.ContextCompat;

import com.fujitsu.pfu.mobile.device.SSDeviceScanSettings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import android_serialport_api.SerialPort;
import io.flutter.embedding.android.FlutterActivity;
import io.flutter.embedding.engine.FlutterEngine;
import io.flutter.plugin.common.EventChannel;
import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel;

public class MainActivity extends FlutterActivity implements
        MethodChannel.MethodCallHandler
        {
    private final String TAG = "Phi_Debugger";
    private static final String PORT_NAME = "/dev/ttyS5";
    private InputStream inputStream;
    private Thread readThread;

    private static final String INTENT_ACTION_GRANT_USB = BuildConfig.APPLICATION_ID + ".GRANT_USB";

     final BroadcastReceiver rs232PermissionGrant;
     final Handler mainLooper = new Handler(Looper.getMainLooper());

    private boolean rs232Connected = false;

    private final RS232Handler rs232Handler = new RS232Handler(this::startReadRS232, this::stopReadRS232);

    private final ScanSnapHandler scanSnapHandler = new ScanSnapHandler();

    String connectDriverFailure = "FF FF FF FF FF";
    String connectDriverSuccess = "00 00 00 00 00";

    private ScanSnapBroadcastManager scanSnapBroadcastManager;

    private static  int connectRS232Count = 0;


    public MainActivity(){
        rs232PermissionGrant = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                if(INTENT_ACTION_GRANT_USB.equals(intent.getAction())) {
                    connectToRS232();
                }
            }
        };
    }

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        connectToRS232();
        Context appContext = getApplicationContext();
        scanSnapBroadcastManager = new ScanSnapBroadcastManager(appContext, scanSnapHandler);
    }

    @SuppressLint("WrongConstant")
    @Override
    protected void onStart() {
        ContextCompat.registerReceiver(getActivity(), rs232PermissionGrant, new IntentFilter(INTENT_ACTION_GRANT_USB), ContextCompat.RECEIVER_NOT_EXPORTED);
        super.onStart();

    }

    @Override
    protected void onResume() {
        scanSnapBroadcastManager.searchForDevices();
        super.onResume();
    }
    @Override
    protected void onStop() {
        getActivity().unregisterReceiver(rs232PermissionGrant);
        scanSnapBroadcastManager.disconnectDevice();
        super.onStop();
    }

    private void connectToRS232() {
        connectRS232Count += 1;
        Log.d(TAG,"Connect RS232 Count: "+ connectRS232Count);

        if(rs232Connected){
            rs232Handler.sendDataToUI(connectDriverSuccess);
        }else {
            try {
                File file = new File(PORT_NAME);
                if(file.exists()){
                    SerialPort serialPort = new SerialPort(file, 9600, 1, 8, 0, 0, 0);
                    inputStream = serialPort.getInputStream();
                    if(inputStream != null){
                        rs232Connected = true;
                        rs232Handler.sendDataToUI(connectDriverSuccess);
                    }
                }else {
                    rs232Handler.sendDataToUI(connectDriverFailure);
                }

            } catch (IOException e) {
                rs232Handler.sendDataToUI(connectDriverFailure);
            }
        }


    }

    private void initializeReadThreadRS232(){
        if(readThread == null ||  readThread.isInterrupted()){
            readThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        byte[] buffer = new byte[1024];
                        int size;
                        while (!Thread.currentThread().isInterrupted()) {
                            size = inputStream.read(buffer);
                            if (size > 0) {
                                StringBuilder hexString = new StringBuilder();
                                for (int i = 0; i < size; i++) {
                                    String hex = Integer.toHexString(buffer[i] & 0xFF);
                                    if (hex.length() == 1) {
                                        hexString.append('0');
                                    }
                                    hexString.append(hex).append(' ');
                                }
                                String receivedDataHex = hexString.toString().trim();
                                Log.d(TAG, "Received data (hex): " + receivedDataHex);
                                if (!receivedDataHex.isEmpty()){
                                    mainLooper.post(
                                            new Runnable() {
                                                @Override
                                                public void run() {
                                                    rs232Handler.sendDataToUI(receivedDataHex);
                                                }
                                            }
                                    );
                                }
                            }
                        }
                    } catch (Exception e) {
                        Log.e(TAG, "Error reading from serial port: " + e.getMessage());
                    }
                }
            });
        }

    }

    private void startReadRS232() {
        initializeReadThreadRS232();
        readThread.start();
    }

    private void stopReadRS232() {
        readThread.interrupt();
    }


    public void writeRS232(String hexString) {
        try {
            Log.d(TAG, "Sent data (hex): " + hexString);
            byte[] hexBytes = hexStringToByteArray(hexString);
            FileOutputStream fos = new FileOutputStream(PORT_NAME);
            fos.write(hexBytes);
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] hexStringToByteArray(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    @Override
    public void configureFlutterEngine(@NonNull FlutterEngine flutterEngine) {
        super.configureFlutterEngine(flutterEngine);
        new MethodChannel(flutterEngine.getDartExecutor().getBinaryMessenger(),"com.mvc.sampling_machine_mobile_testing/send").setMethodCallHandler(this);
        new EventChannel(flutterEngine.getDartExecutor().getBinaryMessenger(), "com.mvc.sampling_machine_mobile_testing/usb_listen").setStreamHandler(rs232Handler);
        new EventChannel(flutterEngine.getDartExecutor().getBinaryMessenger(),  "com.mvc.sampling_machine_mobile_testing/scan_listen").setStreamHandler(scanSnapHandler);
    }

    public  boolean startScanSession(){
        try{
            scanSnapBroadcastManager.startScanSession();
            return true;
        }catch (Exception e){
            return false;
        }
    }
    @Override
    public void onMethodCall(@NonNull MethodCall methodCall, @NonNull MethodChannel.Result result) {
          switch (methodCall.method){

              case "write":
                  try {
                      writeRS232(methodCall.arguments());
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "startScanSession":
                  result.success(startScanSession());
                  break;
              /// scan bill set method
              case "setBlankRemove":
                  try {
                      int blankRemove = methodCall.arguments();
                      scanSnapBroadcastManager.setBlankRemove(blankRemove);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());                  }
                  break;
              case "setBleedThrough":
                  try {
                      int bleedThrough =methodCall.arguments();
                      scanSnapBroadcastManager.setBleedThrough(bleedThrough);    
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setBrightness":
                  try {
                      int brightness = methodCall.arguments();
                      scanSnapBroadcastManager.setBrightness(brightness);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }

                  break;
              case "setColorMode":
                  try {
                      int colorMode = methodCall.arguments();
                      scanSnapBroadcastManager.setColorMode(colorMode);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }

                  break;
              case "setCompression":
                  try {
                      int compression = methodCall.arguments();
                      scanSnapBroadcastManager.setCompression(compression);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }

                  break;
              case "setEDocMode":
                  try {
                      int eDocMode = methodCall.arguments();
                      scanSnapBroadcastManager.setEDocMode(eDocMode);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }

                  break;
              case "setFeedMode":
                  try {
                      int feedMode = methodCall.arguments();
                      scanSnapBroadcastManager.setFeedMode(feedMode);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setFileFormat":
                  try {
                      int fileFormat = methodCall.arguments();
                      scanSnapBroadcastManager.setFileFormat(fileFormat);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setImageQuality":
                  try {
                      int imageQuality = methodCall.arguments();
                      scanSnapBroadcastManager.setImageQuality(imageQuality);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setMultiFeed":
                  try {
                      int multiFeed = methodCall.arguments();
                      scanSnapBroadcastManager.setMultiFeed(multiFeed);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setOutputPath":
                  try {
                      String outputPath = methodCall.arguments();
                      scanSnapBroadcastManager.setOutputPath(outputPath);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setPaperProtection":
                  try {
                      int paperProtection = methodCall.arguments();
                      scanSnapBroadcastManager.setPaperProtection(paperProtection);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setPaperSize":
                  try {
                      int paperSize = methodCall.arguments();
                      scanSnapBroadcastManager.setPaperSize(paperSize);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setPassword":
                  try {
                      String password = methodCall.arguments();
                      scanSnapBroadcastManager.setPassword(password);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              case "setScanSide":
                  try {
                      int scanSide = methodCall.arguments() ;
                      scanSnapBroadcastManager.setScanSide(scanSide);
                      result.success(true);
                  }catch (Exception e){
                      result.error("402", e.toString(), e.toString());
                  }
                  break;
              /// scan bill get method
              case "getBlankRemove":
                  result.success(scanSnapBroadcastManager.getBlankRemove());
                  break;
              case "getBleedThrough":
                  result.success(scanSnapBroadcastManager.getBleedThrough());
                  break;
              case "getBrightness":
                  result.success(scanSnapBroadcastManager.getBrightness());
                  break;
              case "getColorMode":
                  result.success(scanSnapBroadcastManager.getColorMode());
                  break;
              case "getCompression":
                  result.success(scanSnapBroadcastManager.getCompression());
                  break;
              case "getEDocMode":
                  result.success(scanSnapBroadcastManager.getEDocMode());
                  break;
              case "getFeedMode":
                  result.success(scanSnapBroadcastManager.getFeedMode());
                  break;
              case "getFileFormat":
                  result.success(scanSnapBroadcastManager.getFileFormat());
                  break;
              case "getImageQuality":
                  result.success(scanSnapBroadcastManager.getImageQuality());
                  break;
              case "getMultiFeed":
                  result.success(scanSnapBroadcastManager.getMultiFeed());
                  break;
              case "getOutputPath":
                  result.success(scanSnapBroadcastManager.getOutputPath());
                  break;
              case "getPaperProtection":
                  result.success(scanSnapBroadcastManager.getPaperProtection());
                  break;
              case "getPaperSize":
                  result.success(scanSnapBroadcastManager.getPaperSize());
                  break;
              case "getPassword":
                  result.success(scanSnapBroadcastManager.getPassword());
                  break;
              case "getScanSide":
                  result.success(scanSnapBroadcastManager.getScanSide());
                  break;

              default:
                  result.notImplemented();



          }
    }

}

