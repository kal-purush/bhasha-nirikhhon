package edu.uw.servicedemo;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;

public class MainActivity extends AppCompatActivity {

    private static final String TAG = "Main";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
    }

    @Override
    protected void onStart() {
        super.onStart();
    }

    @Override
    protected void onStop() {
        super.onStop();
    }

    //when "Start" button is pressed
    public void handleStart(View v){
        Log.i(TAG, "Start pressed");

    }

    //when "Stop" button is pressed
    public void handleStop(View v){
        Log.i(TAG, "Stop pressed");

    }


    /* Media controls */
    public void playMedia(View v){

    }

    public void pauseMedia(View v){

    }

    public void stopMedia(View v){

    }


    //when "Quit" button is pressed
    public void handleQuit(View v){
        finish(); //end the Activity
    }

    @Override
    protected void onDestroy() {
        Log.v(TAG, "Activity destroyed");
        super.onDestroy();
    }

}