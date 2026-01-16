package com.example.jennahuston.activityrecognitiontry;

import android.app.Service;
import android.content.Intent;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;
import android.os.Binder;
import android.os.Handler;
import android.os.IBinder;

import java.util.Collections;
import java.util.LinkedList;

public class BoundedService extends Service implements SensorEventListener {

    SensorManager sensorManager;
    Sensor accelerometer;
    private final static int DELAY=100;
    private Handler myHandler = new Handler();
    private double rms;

    LinkedList<Double> listX;
    LinkedList<Double> listY;
    LinkedList<Double> listZ;

    @Override
    public void onDestroy() {
        super.onDestroy();
        sensorManager.unregisterListener(this, accelerometer);
    }

    public BoundedService() {

    }

    @Override
    public void onCreate() {
        super.onCreate();
        sensorManager = (SensorManager) getSystemService(SENSOR_SERVICE);
        accelerometer = sensorManager.getDefaultSensor(Sensor.TYPE_ACCELEROMETER);
        sensorManager.registerListener(this, accelerometer, SensorManager.SENSOR_DELAY_NORMAL, DELAY);

        listX = new LinkedList<Double>();
        listY = new LinkedList<Double>();
        listZ = new LinkedList<Double>();
    }

    @Override
    public void onSensorChanged(SensorEvent event) {
        Sensor mySensor = event.sensor;

        switch (mySensor.getType()) {
            case Sensor.TYPE_ACCELEROMETER:
                myHandler.post(new AcclWork(event));
                break;
        }
    }

    @Override
    public void onAccuracyChanged(Sensor sensor, int accuracy) {

    }

    private MyBinder mybinder_ = new MyBinder();

    @Override
    public IBinder onBind(Intent intent) {
        // TODO: Return the communication channel to the service.

        return mybinder_;

    }

    public synchronized double getRms() {

        return rms;
    }

    private class AcclWork implements Runnable {
        private SensorEvent event_;

        public AcclWork(SensorEvent event) {
            event_ = event;
        }

        //Add code to get and process data for activity recognition
        @Override
        public void run() {
            double acclx = event_.values[0];
            double accly = event_.values[1];
            double acclz = event_.values[2];

            listX.add((Double) acclx);
            listY.add((Double) accly);
            listZ.add((Double) acclz);

            if(listX.size() > 100) {
                listX.removeFirst();
                listY.removeFirst();
                listZ.removeFirst();

                double sumX = 0;
                double sumY = 0;
                double sumZ = 0;
                for(int i = 0; i < listX.size(); i++) {
                    sumX += listX.get(i);
                    sumY += listY.get(i);
                    sumZ += listZ.get(i);
                }

                sumX /= 100;
                sumY /= 100;
                sumZ /= 100;

                rms = Math.sqrt((sumX * sumX + sumY * sumY + sumZ * sumZ)/3);
            }

        }
    }

    public class MyBinder extends Binder {

        BoundedService getService() {
            return BoundedService.this;
        }
    }

    public String msg(){
        return "Hello World";
    }


}