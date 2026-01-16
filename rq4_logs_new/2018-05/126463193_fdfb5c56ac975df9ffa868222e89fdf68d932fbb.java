package com.example.cwgj.alibabapush;

import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.support.v4.app.NotificationCompat;
import android.util.Log;
import android.widget.RemoteViews;

import com.alibaba.sdk.android.push.MessageReceiver;
import com.alibaba.sdk.android.push.notification.CPushMessage;
import com.example.cwgj.pushlib.R;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static android.content.Context.NOTIFICATION_SERVICE;
import static com.alibaba.sdk.android.ams.common.global.AmsGlobalHolder.getPackageName;

/**
 * +----------------------------------------------------------------------
 * |  说明     ：
 * +----------------------------------------------------------------------
 * | 创建者   :  ldw
 * +----------------------------------------------------------------------
 * | 时　　间 ：2018/3/21 17:37
 * +----------------------------------------------------------------------
 * | 版权所有: 北京市车位管家科技有限公司
 * +----------------------------------------------------------------------
 **/
public class MyMessageReceiver extends MessageReceiver {
    // 消息接收部分的LOG_TAG
    @Override
    public void onNotification(Context context, String title, String summary, Map<String, String> extraMap) {
        Log.e("MyMessageReceiver", "Receive notification, title: " + title + ", summary: " + summary + ", extraMap: " + extraMap);

    }
    @Override
    public void onMessage(Context context, CPushMessage cPushMessage) {
        Log.e("MyMessageReceiver", "onMessage, messageId: " + cPushMessage.getMessageId() + ", title: " + cPushMessage.getTitle() + ", content:" + cPushMessage.getContent());
    }
    @Override
    public void onNotificationOpened(Context context, String title, String summary, String extraMap) {
        Log.e("MyMessageReceiver", "onNotificationOpened, title: " + title + ", summary: " + summary + ", extraMap:" + extraMap);
    }
    @Override
    protected void onNotificationClickedWithNoAction(Context context, String title, String summary, String extraMap) {
        Log.e("MyMessageReceiver", "onNotificationClickedWithNoAction, title: " + title + ", summary: " + summary + ", extraMap:" + extraMap);
    }
    @Override
    protected void onNotificationReceivedInApp(Context context, String title, String summary, Map<String, String> extraMap, int openType, String openActivity, String openUrl) {
        Log.e("MyMessageReceiver", "onNotificationReceivedInApp, title: " + title + ", summary: " + summary + ", extraMap:" + extraMap + ", openType:" + openType + ", openActivity:" + openActivity + ", openUrl:" + openUrl);
    }
    @Override
    protected void onNotificationRemoved(Context context, String messageId) {
        Log.e("MyMessageReceiver", "onNotificationRemoved");
    }

    /**
     * 接受到对应消息后，消息的弹出处理
     */
    public void buildNotification(Context context, CPushMessage message) {
        NotificationManager notificationManager = (NotificationManager) context.getSystemService(NOTIFICATION_SERVICE);
        RemoteViews remoteViews = new RemoteViews(getPackageName(), R.layout.notification_view);
        remoteViews.setImageViewResource(R.id.custom_icon, R.drawable.ic_launcher);
        remoteViews.setTextViewText(R.id.tv_custom_title, message.getTitle());
        remoteViews.setTextViewText(R.id.tv_custom_content, message.getContent());
        remoteViews.setTextViewText(R.id.tv_custom_time, new SimpleDateFormat("HH:mm").format(new Date()));
        Notification notification = new NotificationCompat.Builder(context)
                .setContent(remoteViews)
                .setContentTitle(message.getTitle())
                .setContentText(message.getContent())
                .setSmallIcon(R.drawable.ic_launcher)
                .setDefaults(Notification.DEFAULT_VIBRATE)
                .setPriority(Notification.PRIORITY_DEFAULT)
                .build();
        notification.contentIntent = buildClickContent(context, message);
        notification.deleteIntent = buildDeleteContent(context, message);
        notificationManager.notify(message.hashCode(), notification);
    }
    public PendingIntent buildClickContent(Context context, CPushMessage message) {
        Intent clickIntent = new Intent();
        clickIntent.setAction("com.cwgj.build");
        //添加其他数据
        clickIntent.putExtra("message key",  message);//将message放入intent中，方便通知自建通知的点击事件
        return PendingIntent.getService(context, 1, clickIntent, PendingIntent.FLAG_UPDATE_CURRENT);
    }
    public PendingIntent buildDeleteContent(Context context, CPushMessage message) {
        Intent deleteIntent = new Intent();
        deleteIntent.setAction("com.cwgj.delete");
        //添加其他数据
        deleteIntent.putExtra("message key",  message);//将message放入intent中，方便通知自建通知的点击事件
        return PendingIntent.getService(context, 2, deleteIntent, PendingIntent.FLAG_UPDATE_CURRENT);
    }
}