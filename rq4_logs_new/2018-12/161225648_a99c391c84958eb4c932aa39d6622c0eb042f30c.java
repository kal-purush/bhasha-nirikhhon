package com.dam.asfaltame;

import android.app.Notification;
import android.app.PendingIntent;
import android.app.TaskStackBuilder;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.support.v4.app.NotificationCompat;
import android.support.v4.app.NotificationManagerCompat;

import com.dam.asfaltame.Activities.ReportDetailActivity;
import com.dam.asfaltame.Database.AppRepository;
import com.dam.asfaltame.Model.Report;

public class AsfaltameBroadcastReceiver extends BroadcastReceiver {
    @Override
    public void onReceive(final Context context, final Intent intent){
        final AppRepository ap = AppRepository.getInstance(context);
        final int reportId = intent.getExtras().getInt("reportId");

        Runnable r = new Runnable() {
            @Override
            public void run() {
                Report r = ap.reportDao.getById(reportId).get(0);

                Intent reportDetailIntent = new Intent(context,ReportDetailActivity.class);
                reportDetailIntent.putExtra("report", r);
                TaskStackBuilder stackBuilder = TaskStackBuilder.create(context);
                stackBuilder.addNextIntentWithParentStack(reportDetailIntent);
                PendingIntent reportDetailPendingIntent = stackBuilder.getPendingIntent(0,PendingIntent.FLAG_UPDATE_CURRENT);

                // TODO: Agregar icono
                Notification notification = new Notification();
                if (intent.getAction() == "com.dam.asfaltame.REPARADO"){
                    notification = new NotificationCompat.Builder(context,"CANAL01")
                            .setContentTitle("TU RECLAMO FUE SOLUCIONADO")
                            .setStyle(new NotificationCompat.BigTextStyle().bigText(
                                    "Tu reclamo ubicado en " + r.getAddress() + "fue solucionado."
                            ))
                            .setContentIntent(reportDetailPendingIntent)
                            .build();
                } else if (intent.getAction() == "com.dam.asfaltame.EN_REPARACION"){
                    notification = new NotificationCompat.Builder(context,"CANAL01")
                            .setContentTitle("TU RECLAMO ESTA EN REPARACION")
                            .setStyle(new NotificationCompat.BigTextStyle().bigText(
                                    "Tu reclamo ubicado en " + r.getAddress() + "está siendo reparado."
                            ))
                            .setContentIntent(reportDetailPendingIntent)
                            .build();
                }
                NotificationManagerCompat notificationManager = NotificationManagerCompat.from(context);
                notificationManager.notify(1,notification);
            }
        };

        Thread t = new Thread(r);
        t.start();
    }
}