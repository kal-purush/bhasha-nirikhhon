package com.example.justpoteito.network.request;

import com.example.justpoteito.models.CuisineType;
import com.example.justpoteito.network.NetConfiguration;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

public class DeleteUserRequest extends NetConfiguration implements Runnable {

    private final String theUrl = theBaseUrl + "/auth/users/";
    private String response;
    private int userId;

    public DeleteUserRequest(int userId) {
        this.userId = userId;
    }


    @Override
    public void run() {
        try {
            URL url = new URL(theUrl + userId);
            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod( "POST" );
            int responseCode = httpURLConnection.getResponseCode();


            if(responseCode == HttpURLConnection.HTTP_NO_CONTENT){
                this.response = "User deleted";

            } else if (responseCode == HttpURLConnection.HTTP_CONFLICT) {
                this.response = "This user does not exist";
            } else {
                this.response = "The user could not be deleted";
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String getResponse() {
        return response;
    }
}