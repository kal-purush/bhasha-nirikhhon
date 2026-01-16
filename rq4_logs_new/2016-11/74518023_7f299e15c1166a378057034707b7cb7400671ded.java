package com.pinnackl.shoppinglist;

import org.json.JSONObject;

/**
 * Created by htutuaku on 28/11/2016.
 */

public interface IHttpRequestListener {
    void onFailure(String errorMessage);
    void onSuccess(JSONObject result);
}