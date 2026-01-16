package com.example.drcreeper.alexweather.models;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;

import com.example.drcreeper.alexweather.models.generated.Cities;
import com.example.drcreeper.alexweather.utils.Utils;
import com.google.gson.Gson;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;

public class ChacheService extends Service {
    public ChacheService() {
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    InputStreamReader reader = new InputStreamReader(getAssets().open(Utils.JSON_FILE_NAME));
                    FileOutputStream outputStream = new FileOutputStream(new File(getCacheDir(),Utils.DUMP_FILE_NAME));
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                    Gson gson = new Gson();
                    Cities cities = gson.fromJson(reader, Cities.class);
                    if(reader != null){
                        reader.close();
                    }
                    objectOutputStream.writeObject(cities);
                    if(objectOutputStream != null){
                        objectOutputStream.close();
                    }
                    if(outputStream != null){
                        outputStream.close();
                    }
                    outputStream = new FileOutputStream(new File(getCacheDir(),Utils.LIST_FILE_NAE));
                    objectOutputStream = new ObjectOutputStream(outputStream);
                    objectOutputStream.writeObject(cities.getNames());
                    if(objectOutputStream != null){
                        objectOutputStream.close();
                    }
                    if(outputStream != null){
                        outputStream.close();
                    }
                }catch (IOException e){
                    stopSelf();
                }finally {
                    stopSelf();
                }
            }
        }).start();

        return super.onStartCommand(intent, flags, startId);
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }
}