package com.example.nermingraca.bitbayapp.singletons;

import android.util.Log;

import com.example.nermingraca.bitbayapp.R;
import com.example.nermingraca.bitbayapp.service.ServiceRequest;
import com.squareup.okhttp.Callback;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by nermingraca on 29/04/15.
 */
public class CartFeed {
    private static CartFeed ourInstance = new CartFeed();

    public static CartFeed getInstance() {
        return ourInstance;
    }

    private String mCartList;

    private CartFeed() {

    }

    public String getCartList(){
        return mCartList;
    }

    public void getCartFeed(){
        int buyerId = UserData.getInstance().getId();
        String url = AppController.getContext().getResources().getString(R.string.service_get_cart);
        JSONObject json = new JSONObject();

        try {
            json.put("userId", buyerId);
        } catch (JSONException e) {
            e.printStackTrace();
            Log.e("ERROR", e.getMessage());
        }
        String jsonString = json.toString();
        Log.d("DEBUG", jsonString);
        Callback callback = response();
        ServiceRequest.post(url, jsonString, callback);
    }

    private Callback response() {
        return new Callback() {

            @Override
            public void onFailure(Request request, IOException e) {
                Log.e("ERROR", e.getMessage());
            }

            @Override
            public void onResponse(Response response) throws IOException {
                mCartList = response.body().string();
                Log.d("DEBUG in Cart Feed", mCartList);

            }
        };
    }
}