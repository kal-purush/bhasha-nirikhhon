package com.example.android.bluetoothlegatt;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.util.Log;

/**
 * 这个接收器需要在程序内动态注册，并调用它的构造器来初始化
 *具体代码如下：

 //先声明
 private NotificationReceiver mNotificationReceiver;
 //在地址和密码都确定了的时候创建一个接收器实例并注册。请注意构造器的参数类型，地址是内容为标准地址格式的String类型，密码是长度为8的byte数组
 mNotificationReceiver = new NotificationReceiver(mDeviceAddress, mDevicePassword);
 private static IntentFilter mIntentFilter(){
 final IntentFilter intentFilter = new IntentFilter();
 intentFilter.addAction("android.intent.action.PHONE_STATE");
 intentFilter.addAction("android.provider.Telephony.SMS_RECEIVED");
 return intentFilter;
 }
 registerReceiver(mNotificationReceiver, mIntentFilter);
 //别忘了在结束的时候注销接收器
 unregisterReceiver(mNotificationReceiver);

 */
public class NotificationReceiver extends BroadcastReceiver {
    private static final String TAG = "NOTIFICATION RECEIVER";

    private String mDeviceAddress;
    private byte[] mDevicePassword;

    public NotificationReceiver(String deviceAddress, byte[] devicePassword) {
        if(deviceAddress != null) mDeviceAddress = deviceAddress;
        else Log.w(TAG, "ERROR: device address is null!");
        if(devicePassword.length == 8) mDevicePassword = devicePassword;
        else Log.w(TAG, "ERROR: password length is not 8!");
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        Intent service = new Intent(context, BluetoothLeService.class);
        service.putExtra("DEVICE_ADDRESS", mDeviceAddress);
        service.putExtra("DEVICE_PASSWORD", mDevicePassword);
        context.startService(service);
    }
}