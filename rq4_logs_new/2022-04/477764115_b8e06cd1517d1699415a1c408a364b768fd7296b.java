package com.example.geomessages.http;

import android.content.Context;
import android.util.Log;

import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.toolbox.JsonArrayRequest;
import com.android.volley.toolbox.Volley;
import com.example.geomessages.model.Message;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

public class VolleyUtils {
    private ArrayList<Message> messages = new ArrayList<>();

    public interface ListMessagesAsyncResponse {
        void processFinished(ArrayList<Message> messagesArrayList);
    }


    public void getMessages(Context context, ListMessagesAsyncResponse callback) {
        String url = "https://onoup.site/data.json";
        RequestQueue queue = Volley.newRequestQueue(context);

        JsonArrayRequest jsonArrayRequest = new JsonArrayRequest
                (Request.Method.GET, url, null, response ->
                {
                    for (int i = 0; i < response.length(); i++) {
                        Message message = new Message();
                        try {
                            JSONObject jsonObject = response.getJSONObject(i);
                            message.setFirstname(jsonObject.getString("firstname"));
                            message.setLastname(jsonObject.getString("lastname"));
                            message.setLongitude(jsonObject.getString("longitude"));
                            message.setLatitude(jsonObject.getString("latitude"));
                            message.setPicture(jsonObject.getString("picture"));
                            message.setMessage(jsonObject.getString("message"));
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        messages.add(message);
                    }
                    if (callback != null) callback.processFinished(messages);
                },
                        error -> Log.d("TAG", "error: " + error));
        queue.add(jsonArrayRequest);
    }
}