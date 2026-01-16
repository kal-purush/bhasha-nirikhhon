package com.intbridge.projects.gaucholife.utils;

import android.os.AsyncTask;
import android.util.Log;

import com.intbridge.projects.gaucholife.PGDatabaseManager;

import java.util.Date;
import java.util.Map;

/**
 *
 * Created by Derek on 1/9/2017.
 */

public class DinningDataUpload {
    private int totalLoadedDays = 10;
    private PGDatabaseManager databaseManager;

    public DinningDataUpload(int totalLoadedDays) {
        this.totalLoadedDays = totalLoadedDays;
        this.databaseManager = new PGDatabaseManager();
    }

    public void uploadParseServer(int offsetDays) {
        if(databaseManager == null){
            databaseManager = new PGDatabaseManager();
        }
        Date date = databaseManager.addDays(new Date(),offsetDays);
        new DinningDataUploadTask().execute(date);
    }

    private class DinningDataUploadTask extends AsyncTask<Date, Integer, Map<String, Map>> {

        Date currentDate;
        @Override
        protected void onPreExecute() {
            if(databaseManager == null){
                databaseManager = new PGDatabaseManager();
            }
        }

        @Override
        protected Map<String, Map> doInBackground(Date... params) {
            // params comes from the execute() call: use params[0] for the first.
            currentDate = params[0];
            Map<String, Map> result = databaseManager.getUCSBCommonsDataFromHTML(currentDate);
            int dateInt = databaseManager.convertDateToInteger(currentDate);
            databaseManager.getParseObjectFromHTML(dateInt, result);
            return result;
        }

        // onPostExecute displays the results of the AsyncTask.
        @Override
        protected void onPostExecute(Map<String, Map> result) {
            Log.e("Mcurrent: ", databaseManager.convertDateToInteger(currentDate) + "");
            totalLoadedDays--;
            if(totalLoadedDays > 0){
                currentDate = databaseManager.addDays(currentDate,1);
                new DinningDataUploadTask().execute(currentDate);
            }
        }
    }
}
