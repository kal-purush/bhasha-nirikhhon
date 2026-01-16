package io.realm.dagger1.example;

import android.app.Application;

import dagger.ObjectGraph;
import io.realm.dagger1.example.modules.NetworkModule;

/**
 * Created by TheFinestArtist on 6/15/15.
 */
public class BaseApplication extends Application {

    private ObjectGraph objectGraph;

    @Override public void onCreate(){
        super.onCreate();
        objectGraph = ObjectGraph.create(
                new NetworkModule()
        );
    }

    public ObjectGraph getObjectGraph(){
        return objectGraph;
    }
}