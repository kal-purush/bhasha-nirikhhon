package com.example.spacetrader.model;

import android.arch.lifecycle.MutableLiveData;

import com.example.spacetrader.entity.Player;

/**
 * Container for the TOTAL STATE of the application.
 * Singleton model (Only one instance of Model in total application)
 * Contains all Interactors for application
 */

public class Model {
    private Repository gameRepo;
    //Singleton instance
    private static Model instance;

    public static Model getInstance() {
        if(instance == null){
            instance = new Model();
        }
        return instance;
    }

    //Initialize model
    private Model(){
        this.gameRepo = new Repository();
    }

    public MutableLiveData<Repository> getRepository(){
        MutableLiveData<Repository> data = new MutableLiveData<>();
        data.setValue(gameRepo);
        return data;
    }
}