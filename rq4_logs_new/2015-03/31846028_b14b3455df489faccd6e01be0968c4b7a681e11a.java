package com.example.tamas.callmanager;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.telephony.PhoneStateListener;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.Toast;

import java.lang.reflect.Method;

/**
 * Created by Tram on 2015.03.15..
 */
public class IncomingCall extends BroadcastReceiver {
    TelephonyManager tmgr;

    @Override
    public void onReceive(Context context, Intent intent) {
        try {
            // TELEPHONY MANAGER class object to register one listner
            tmgr = (TelephonyManager) context
                    .getSystemService(Context.TELEPHONY_SERVICE);

            //Create Listner
            MyPhoneStateListener PhoneListener = new MyPhoneStateListener();

            // Register listener for LISTEN_CALL_STATE
            tmgr.listen(PhoneListener, PhoneStateListener.LISTEN_CALL_STATE);

        } catch (Exception e) {
            Log.e("Phone Receive Error", " " + e);
        }
    }

    private class MyPhoneStateListener extends PhoneStateListener {
        public void onCallStateChanged(int state, String incomingNumber) {

            Log.d("MyPhoneListener", state + "   incoming no:" + incomingNumber);
            //rejectCall();

            if (state == 1) {

                String msg = "New Phone Call Event. Incomming Number : " + incomingNumber;
                int duration = Toast.LENGTH_LONG;
                //Log.d("MyPhoneListener",state+"   incoming no:"+incomingNumber);

            }
        }
    }

    //bejövő hívás lerakása
    private void rejectCall() {
        try {
            // Get the getITelephony() method
            Class<?> classTelephony = Class.forName(tmgr.getClass().getName());
            Method method = classTelephony.getDeclaredMethod("getITelephony");
            // Disable access check
            method.setAccessible(true);

            // Invoke getITelephony() to get the ITelephony interface
            Object telephonyInterface = method.invoke(tmgr);
            // Get the endCall method from ITelephony
            Class<?> telephonyInterfaceClass = Class.forName(telephonyInterface.getClass().getName());
            Method methodEndCall = telephonyInterfaceClass.getDeclaredMethod("endCall");
            // Invoke endCall()
            methodEndCall.invoke(telephonyInterface);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}