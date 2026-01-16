package com.dtavana.foodswipe.network;

import android.util.Log;

import com.codepath.asynchttpclient.AsyncHttpClient;
import com.codepath.asynchttpclient.RequestParams;
import com.codepath.asynchttpclient.callback.JsonHttpResponseHandler;

public class FoodSwipeClient extends AsyncHttpClient {

    public static final String TAG = "FoodSwipeClient";
    public static final String BASE_URL = "https://opentable.herokuapp.com/api/";

    public void getRestaurants(String city, JsonHttpResponseHandler handler) {
        // TODO: Decide whether to use ZipCode or City
        RequestParams params = new RequestParams();
        params.put("city", "Cambridge");
        String apiUrl = getApiUrl("restaurants");
        Log.d(TAG, "getRestaurants: Resolved apiUrl: " + apiUrl);
        get(apiUrl, params, handler);
    }

    public void getRestaurant(String restaurantId, JsonHttpResponseHandler handler) {
        String apiUrl = getApiUrl("restaurants" + restaurantId);
        Log.d(TAG, "getRestaurant: Resolved apiUrl: " + apiUrl);
        get(apiUrl, null, handler);
    }

    private String getApiUrl(String endpoint) {
        return String.format("%s%s", BASE_URL, endpoint);
    }
}