package com.operationcodify.cavoid.activities;

import android.app.Application;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.lifecycle.AndroidViewModel;
import androidx.lifecycle.MutableLiveData;

import com.android.volley.Response;
import com.operationcodify.cavoid.api.Repository;
import com.operationcodify.cavoid.database.LocationDao;
import com.operationcodify.cavoid.database.LocationDatabase;
import com.operationcodify.cavoid.database.PastLocation;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

public class GraphActivityViewModel extends AndroidViewModel {
    private LocationDatabase locDb;
    private LocationDao locDao;
    public ArrayList<String> activeCasesEst;
    public String fips;
    public String TAG;
    public int i;
    private Repository repo;
    private MutableLiveData<Integer> counter;
    private ArrayList<String> pastLocations;

    public GraphActivityViewModel(@NonNull Application application){
        super(application);
        locDb = LocationDatabase.getDatabase(getApplication().getApplicationContext());
        locDao = locDb.getLocationDao();
        pastLocations = (ArrayList<String>) locDao.getAllDistinctFips();
        TAG = DashboardActivityViewModel.class.getName();
        repo = new Repository(application.getApplicationContext());
    }

    public MutableLiveData<Integer> getCounter() {
        if(counter == null){
            counter = new MutableLiveData<Integer>();
            counter.setValue(0);
        }
        return counter;
    }

    public void updatePastLocationMessages(){
        for(i = 0; i < pastLocations.size();i++) {
            repo.getPosTests(pastLocations.get(i), new Response.Listener<JSONObject>() {
                @Override
                public void onResponse(JSONObject response) {
                    try {
                        activeCasesEst.add(response.getString("active_cases_est"));
                        counter.setValue(counter.getValue() + 1);
                    }
                    catch(JSONException j){
                        Log.i(TAG,"Could not get a response!");
                    }
                }
            });
        }
    }
}