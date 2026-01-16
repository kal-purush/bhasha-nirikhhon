package com.translate.forsenboyz.rise42.translate;

import android.Manifest;
import android.content.pm.PackageManager;
import android.os.AsyncTask;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.widget.Toast;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.translate.forsenboyz.rise42.translate.MainActivity.TAG;

public class BackupActivity extends AppCompatActivity {

    private static final int PERMISSION_REQUEST = 42;

    private File directoryDB;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_backup);

        checkPermission();
    }

    private void checkPermission(){
        if(ContextCompat.checkSelfPermission(this, Manifest.permission.WRITE_EXTERNAL_STORAGE)
                == PackageManager.PERMISSION_GRANTED){
            new DataBackup().execute();
        }
        else{
            Log.d(TAG, "checkPermission: NO DAMN PERMISSION");
            requestPermissions(
                    new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE},
                    PERMISSION_REQUEST
                    );
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);

        if(requestCode == PERMISSION_REQUEST && grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED){
            Toast.makeText(this,"Nice",Toast.LENGTH_SHORT).show();
            checkPermission();
        } else{
            Toast.makeText(this,"What a digusting person you are",Toast.LENGTH_SHORT).show();
            //checkPermission();
        }
    }

    private class DataBackup extends AsyncTask {

        @Override
        protected Object doInBackground(Object[] params) {

            try {

                File[] externalDirs = getApplicationContext().getExternalFilesDirs(null);

                if(externalDirs != null && externalDirs.length >=2){
                    directoryDB = externalDirs[1];
                    Log.d(TAG, "doInBackground: SDDIR::"+directoryDB.getAbsolutePath());
                } else{
                    Log.d(TAG, "doInBackground: pity");
                    Toast.makeText(BackupActivity.this, "Error", Toast.LENGTH_SHORT).show();
                    publishProgress(null);
                }

                Log.d(TAG, "doInBackground: made a dir");

                SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy_HH..mm");

                File originalDB = getApplicationContext().getDatabasePath(DatabaseHandler.DATABASE_NAME);
                File targetDB = new File(directoryDB, dateFormat.format(new Date())+".sqlite");
                targetDB.createNewFile();

                Log.d(TAG, "target db path: "+targetDB.getAbsolutePath());

                Log.d(TAG, "doInBackground: coping file");

                copyFile(originalDB, targetDB);

                Log.d(TAG, "doInBackground: copied");

                publishProgress(null);

            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

        @Override
        protected void onProgressUpdate(Object[] values) {
            finish();
        }

        @Override
        protected void onPostExecute(Object o) {
            super.onPostExecute(o);
        }

        private void copyFile(File source, File copy) throws IOException {
            FileReader reader = new FileReader(source);
            FileWriter writer = new FileWriter(copy);

            char[] buff = new char[1024];
            int len;
            while ((len = reader.read(buff)) > 0) {
                writer.write(buff, 0, len);
            }

            writer.flush();

            reader.close();
            writer.close();
        }
    }

}