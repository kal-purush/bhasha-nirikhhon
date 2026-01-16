package com.softdesign.devintensive.ui.activities;

import android.os.Bundle;
import android.os.PersistableBundle;

import com.redmadrobot.chronos.ChronosOperation;
import com.redmadrobot.chronos.ChronosOperationResult;
import com.redmadrobot.chronos.gui.activity.ChronosActivity;
import com.softdesign.devintensive.data.managers.DataManager;
import com.softdesign.devintensive.data.network.res.UserModelRes;
import com.softdesign.devintensive.data.storage.models.User;

import java.util.List;

/**
 * Created by alena on 22.07.16.
 */
public class MyChronosActivity extends ChronosOperation<List<User>> {




    public List<User> run(){

        DataManager mDataManager;
        mDataManager = DataManager.getInstance();

        List<User> list = mDataManager.getUserListFromDb();

        return list;
    }
    public Class<? extends ChronosOperationResult<List<User>>> getResultClass() {
        return Result.class;
    }


    public final static class Result extends ChronosOperationResult<List<User>> {

    }
}