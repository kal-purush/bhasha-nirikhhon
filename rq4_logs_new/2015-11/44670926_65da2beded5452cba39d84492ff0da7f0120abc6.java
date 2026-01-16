package com.example.shane.getmeup;

import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;
import android.media.Ringtone;
import android.media.RingtoneManager;
import android.net.Uri;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.TextView;
import android.widget.Toast;

public class PutShakeAlarmOff extends AppCompatActivity implements SensorEventListener {

    int count = 1;
    int noOfShakes = 0;
    private boolean init;
    private Sensor mAccelerometer;
    private SensorManager mSensorManager;
    private float x1, x2, x3;
    private static final float ERROR = (float) 7.0;

    private TextView alarmDescriptionTxtView;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_put_shake_alarm_off);
        noOfShakes = getIntent().getExtras().getInt("NoOfShakes");
        alarmDescriptionTxtView = (TextView) findViewById(R.id.alarmDescription);
        alarmDescriptionTxtView.setText("Shake the phone " + String.valueOf(noOfShakes) + " times to put the alarm off.");
        mSensorManager = (SensorManager) getSystemService(SENSOR_SERVICE);
        mAccelerometer = mSensorManager.getDefaultSensor(Sensor.TYPE_ACCELEROMETER);
        mSensorManager.registerListener(this, mAccelerometer, SensorManager.SENSOR_DELAY_NORMAL);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_put_shake_alarm_off, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    @Override
    public void onSensorChanged(SensorEvent event) {

        //Get x,y and z values
        float x,y,z;
        x = event.values[0];
        y = event.values[1];
        z = event.values[2];


        if (!init) {
            x1 = x;
            x2 = y;
            x3 = z;
            init = true;
        } else {

            float diffX = Math.abs(x1 - x);
            float diffY = Math.abs(x2 - y);
            float diffZ = Math.abs(x3 - z);

            //Handling ACCELEROMETER Noise
            if (diffX < ERROR) {

                diffX = (float) 0.0;
            }
            if (diffY < ERROR) {
                diffY = (float) 0.0;
            }
            if (diffZ < ERROR) {

                diffZ = (float) 0.0;
            }


            x1 = x;
            x2 = y;
            x3 = z;


            //Horizontal Shake Detected!
            if (diffX > diffY) {
                count = count+1;
                alarmDescriptionTxtView.setText(String.valueOf(count));
                if(count >= noOfShakes)
                {
                    Toast.makeText(PutShakeAlarmOff.this, "Alarm Off!", Toast.LENGTH_SHORT).show();
                    if (AlarmReceiver.mediaPlayer.isPlaying()) {
                        AlarmReceiver.mediaPlayer.stop();
                    }
                    mSensorManager.unregisterListener(this);
                }
            }
        }

    }

    @Override
    public void onAccuracyChanged(Sensor sensor, int accuracy) {

    }

    //Register the Listener when the Activity is resumed
    protected void onResume() {
        super.onResume();
        mSensorManager.registerListener(this, mAccelerometer, SensorManager.SENSOR_DELAY_NORMAL);
    }

    //Unregister the Listener when the Activity is paused
    protected void onPause() {
        super.onPause();
        mSensorManager.unregisterListener(this);
    }

}