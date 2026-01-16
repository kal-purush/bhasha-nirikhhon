package com.example.spacetrader.viewmodel;

import android.arch.lifecycle.LiveData;
import android.arch.lifecycle.MutableLiveData;
import android.arch.lifecycle.ViewModel;

import com.example.spacetrader.entity.Difficulty;
import com.example.spacetrader.model.Model;
import com.example.spacetrader.model.Repository;

public class CreateCharacterBasicInfoViewModel extends ViewModel {
    private MutableLiveData<Repository> mRepository;
    private Model mModel;

    public void init(){
        if(mRepository != null){
            return;
        }
        mModel = Model.getInstance();
        mRepository = mModel.getRepository();
    }

    public LiveData<Repository> getRepository(){
        return mRepository;
    }

    public void setPlayerData(String name, Difficulty difficulty){
        Repository changes = mRepository.getValue();
        changes.getUserPlayer().setName(name);
        changes.getUserPlayer().setDifficulty(difficulty);
        mRepository.setValue(changes);
    }


}