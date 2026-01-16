package com.example.finalproject;

import android.content.Context;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;

/**
 * Created by parda on 2016/12/14.
 */

public class StepDetector implements SensorEventListener {
    public static int CURRENT_STPEPS = 0;
    public static float SENSITIVITY = 10;
    private float lastValue = 0;
    private float scale = 0;
    private float offset;
    private static long end = 0;
    private static long start = 0;

    private float lastDirection = 0;
    private float lastExtremes[] = new float[2];
    private float lastDiff = 0;
    private int lastMatch = -1;

    public StepDetector(Context context) {
        super();
        init();
    }

    private void init() {
        offset = 480 * 0.5f;
        scale = -(480 * 0.5f * (1.0f / (SensorManager.MAGNETIC_FIELD_EARTH_MAX)));
    }

    @Override
    public void onSensorChanged(SensorEvent event) {
        synchronized (this) {
            if (event.sensor.getType() == Sensor.TYPE_ACCELEROMETER) {
                float v = 0;
                for (int i = 0; i < 3; i++) {
                    v += offset + event.values[i] * scale;
                }
                v /= 3;

                float direction = 0;
                if (v > lastValue) {
                    direction = 1;
                } else if (v == lastValue) {
                    direction = 0;
                } else {
                    direction = -1;
                }

                checkDirection(direction);
                lastDirection = direction;
                lastValue = v;
            }
        }
    }

    private void checkDirection(float direction) {
        if (direction == -lastDirection) {
            int extType = direction > 0 ? 0 : 1;
            lastExtremes[extType] = lastValue;
            float diff = Math.abs(lastExtremes[extType]
                    - lastExtremes[1 - extType]);
            if (diff > SENSITIVITY) {
                changeData(diff, extType);
            }
            lastDiff = diff;
        }
    }

    private void changeData(float diff, int extType) {
        if (diff > (lastDiff * 2 / 3) && lastDiff > (diff / 3)
                && (lastMatch != 1 - extType)) {
            end = System.currentTimeMillis();
            if (end - start > 500) {
                CURRENT_STPEPS++;
                lastMatch = extType;
                start = end;
            }
        } else {
            lastMatch = -1;
        }
    }

    @Override
    public void onAccuracyChanged(Sensor sensor, int accuracy) {

    }
}