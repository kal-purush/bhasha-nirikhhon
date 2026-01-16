package com.example.chores;

import android.app.Application;
import android.content.Context;
import android.text.TextUtils;

import com.android.volley.RequestQueue;
import com.android.volley.toolbox.Volley;
import com.android.volley.Request;

public class AppController {

    public static final String TAG = AppController.class.getName();

    private RequestQueue requestQueue;

    private static AppController mInstance;

    public AppController() {
        mInstance = this;
    }

    public static synchronized AppController getInstance() {
        return mInstance;
    }

    public RequestQueue getRequestQueue(Context context) {
        if (requestQueue == null) {
            requestQueue = Volley.newRequestQueue(context);
        }

        return requestQueue;
    }

    public <T> void addToRequestQueue(Request<T> req, String tag, Context context) {
        req.setTag(TextUtils.isEmpty(tag) ? TAG : tag);
        getRequestQueue(context).add(req);
    }

    public void canclePendingReqest(Object tag) {
        if (requestQueue != null) {
            requestQueue.cancelAll(tag);
        }
    }
}