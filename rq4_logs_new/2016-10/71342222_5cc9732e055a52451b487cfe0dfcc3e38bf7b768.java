package com.example.tan_pc.navigationdraweractivity;

import android.app.Activity;
import android.app.ProgressDialog;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Handler;

/**
 * Created by tan-pc on 24/10/2016.
 */

public class WaterfallSplash extends Activity {
    ProgressDialog progressDialog;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.waterfallsplash);
        new LoadViewTask().execute();

    }

    private class LoadViewTask extends AsyncTask<Void, Integer, Void>
    {
        //Before running code in separate thread
        @Override
        protected void onPreExecute()
        {
            progressDialog = ProgressDialog.show(WaterfallSplash.this,"WaterFall Graphic Display...",
                    "Loading data, please wait...", false, false);
        }

        //The code to be executed in a background thread.
        @Override
        protected Void doInBackground(Void... params)
        {
            try
            {


                //Get the current thread's token
                synchronized (this)
                {
                    //Initialize an integer (that will act as a counter) to zero
                    int counter = 0;
                    //While the counter is smaller than four
                    while(counter <= 1)
                    {
                        //Wait 850 milliseconds
                        this.wait(750);
                        //Increment the counter
                        counter++;
                        //Set the current progress.
                        //This value is going to be passed to the onProgressUpdate() method.
                        publishProgress(counter*25);
                    }
                }
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            return null;
        }

        //Update the progress
        @Override
        protected void onProgressUpdate(Integer... values)
        {
            //set the current progress of the progress dialog
            progressDialog.setProgress(values[0]);
        }

        //after executing the code in the thread
        @Override
        protected void onPostExecute(Void result)
        {
            Intent mainIntent = new Intent(WaterfallSplash.this,MainActivity.class);
            WaterfallSplash.this.startActivity(mainIntent);
            progressDialog.dismiss();
            finish();
            //close the progress dialog

        }
    }
}