package com.vladd11.app.robot;

import android.content.Context;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;
import android.util.Log;

public class Compass implements SensorEventListener {
    private static final String TAG = "Compass";
    private CompassListener listener;

    private float[] gravity;
    private float[] geomagnetic;
    private final float[] R = new float[9];
    private final float[] I = new float[9];

    public void start(Context context) {
        final SensorManager sensorManager = (SensorManager) context.getSystemService(Context.SENSOR_SERVICE);
        sensorManager.registerListener(this,
                sensorManager.getDefaultSensor(Sensor.TYPE_ROTATION_VECTOR),
                SensorManager.SENSOR_DELAY_NORMAL);
        /*sensorManager.registerListener(this,
                sensorManager.getDefaultSensor(Sensor.TYPE_ACCELEROMETER),
                SensorManager.SENSOR_DELAY_NORMAL);*/
    }

    @Override
    public void onSensorChanged(SensorEvent event) {
        float[] mRotationMatrixFromVector = new float[9];
        float[] mRotationMatrix = new float[9];
        float[] orientationVals = new float[3];

        SensorManager.getRotationMatrixFromVector(mRotationMatrixFromVector, event.values);
        SensorManager.remapCoordinateSystem(mRotationMatrixFromVector,
                SensorManager.AXIS_X, SensorManager.AXIS_Z,
                mRotationMatrix);
        SensorManager.getOrientation(mRotationMatrix, orientationVals);
        float heading = (float) Math.toDegrees(orientationVals[0]);
        if(heading < 0) {
            heading = 360 + heading;
        }
        listener.onHeadingChanged(heading);
    }

    @Override
    public void onAccuracyChanged(Sensor sensor, int accuracy) {

    }

    public void setListener(CompassListener listener) {
        this.listener = listener;
    }

    public interface CompassListener {
        void onHeadingChanged(float heading);
    }
}