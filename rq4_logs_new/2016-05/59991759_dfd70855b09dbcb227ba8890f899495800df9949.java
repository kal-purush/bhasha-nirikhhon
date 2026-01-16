package cn.fwso.smsfw;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.provider.Telephony;
import android.telephony.SmsManager;
import android.telephony.SmsMessage;
import android.util.Log;

public class SMSReceiver extends BroadcastReceiver {

    Context context;

    public SMSReceiver() {
    }

    @Override
    public void onReceive(Context context, Intent intent) {
        this.context = context;
        String addr = getCurrentPhone();
        Log.d("SMSFWD", "onReceive");
        if (addr.length() > 0 && intent.getAction().equals("android.provider.Telephony.SMS_RECEIVED")) {
            Bundle bundle = intent.getExtras();
            SmsMessage[] messages = null;
            String messageFrom = "";
            String message = "";
            if (bundle != null) {
                try {
                    Object[] pdus = (Object[]) bundle.get("pdus");
                    messages = new SmsMessage[pdus.length];
                    for (int i = 0; i < pdus.length; i++) {
                        messages[i] = SmsMessage.createFromPdu((byte[]) pdus[i]);
                        messageFrom = messages[i].getOriginatingAddress();
                        String body = messages[i].getMessageBody();
                        message += body;
                    }

                    Log.d("SMSFWD", messageFrom + " send msg: " + message);

                    SmsManager smsManager = SmsManager.getDefault();
                    smsManager.sendTextMessage(addr, null,
                            messageFrom + ": " + message, null, null);
                } catch (Exception ex) {
                    Log.e("SMSFWE", ex.getMessage());
                }
            }
        }
    }

    //    @Override
//    public void onReceive(Context context, Intent intent) {
//        if (Telephony.Sms.Intents.SMS_RECEIVED_ACTION.equals(intent.getAction())) {
//            for (SmsMessage smsMessage : Telephony.Sms.Intents.getMessagesFromIntent(intent)) {
//                String messageBody = smsMessage.getMessageBody();
//            }
//        }
//    }
    private String getCurrentPhone() {
        SharedPreferences pref = context.getSharedPreferences(context.getString(R.string.preference_file_key), context.MODE_PRIVATE);
        String phone = pref.getString(context.getString(R.string.preference_key_phone_number), null);
        if (phone != null) {
            return phone;
        }
        return "";
    }
}