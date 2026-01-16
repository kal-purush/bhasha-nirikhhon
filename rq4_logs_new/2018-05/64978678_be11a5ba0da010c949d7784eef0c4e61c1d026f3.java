package com.jsy_jiaobao.main.umengService;

import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.graphics.BitmapFactory;
import android.graphics.Color;
import android.support.v4.app.NotificationCompat;

import com.jsy_jiaobao.main.R;
import com.jsy_jiaobao.main.personalcenter.MessageCenterActivity;
import com.umeng.message.UTrack;
import com.umeng.message.UmengMessageService;
import com.umeng.message.common.UmLog;
import com.umeng.message.entity.UMessage;

import org.android.agoo.common.AgooConstants;
import org.json.JSONObject;

import me.leolin.shortcutbadger.ShortcutBadger;

/**
 * 类名：.class
 * 描述：
 * Created by：刘帅 on 2018/5/23.
 * --------------------------------------
 * 修改内容：
 * 备注：
 * Modify by：
 */

public class UPushIntentService extends UmengMessageService {

    private static final String TAG = UPushIntentService.class.getName();

    @Override
    public void onMessage(Context context, Intent intent) {
        try {
            //可以通过MESSAGE_BODY取得消息体
            String message = intent.getStringExtra(AgooConstants.MESSAGE_BODY);
            UMessage msg = new UMessage(new JSONObject(message));
            UmLog.d(TAG, "message=" + message);      //消息体
            UmLog.d(TAG, "custom=" + msg.custom);    //自定义消息的内容
            UmLog.d(TAG, "title=" + msg.title);      //通知标题
            UmLog.d(TAG, "text=" + msg.text);        //通知内容

            //添加角标
            try {
                if(ShortcutBadger.isBadgeCounterSupported(context)){
                    SharedPreferences sp =getSharedPreferences("messageNum", MODE_PRIVATE);
                    int num =Integer.valueOf(sp.getString("num","0"));
                    num+=1;
                    ShortcutBadger.applyCount(context, num);
                    SharedPreferences.Editor editor = getSharedPreferences("messageNum", MODE_PRIVATE).edit();
                    editor.putString("num",num+"");
                    editor.commit();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            //展示通知
            showNotifications(context, msg);

            // 对完全自定义消息的处理方式，点击或者忽略
            boolean isClickOrDismissed = true;
            if (isClickOrDismissed) {
                //完全自定义消息的点击统计
                UTrack.getInstance(getApplicationContext()).trackMsgClick(msg);
            } else {
                //完全自定义消息的忽略统计
                UTrack.getInstance(getApplicationContext()).trackMsgDismissed(msg);
            }
        } catch (Exception e) {
            UmLog.e(TAG, e.getMessage());
        }
    }
    /**
     * 自定义通知布局
     *
     * @param context 上下文
     * @param msg     消息体
     */
    public void showNotifications(Context context, UMessage msg) {
        NotificationManager mNotificationManager = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        NotificationCompat.Builder builder = new NotificationCompat.Builder(context);
        builder.setContentTitle(msg.title)
                .setContentText(msg.text)
                .setTicker(msg.ticker)
                .setWhen(System.currentTimeMillis())
                .setSmallIcon(R.drawable.ic_launcher)
                .setLargeIcon(BitmapFactory.decodeResource(context.getResources(),R.drawable.ic_launcher))
                .setColor(Color.parseColor("#41b5ea"))
                .setDefaults(Notification.DEFAULT_ALL)
                .setAutoCancel(true);
        Intent intent = new Intent();
        intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        intent.putExtra(MessageCenterActivity.NEWAFFAIRNOTICE, true);
        intent.setClass(context, MessageCenterActivity.class);
        PendingIntent contentIntent = PendingIntent.getActivity(context, 0,
                intent, PendingIntent.FLAG_UPDATE_CURRENT);
        builder.setContentIntent(contentIntent);
        mNotificationManager.notify(100, builder.build());
    }
}

