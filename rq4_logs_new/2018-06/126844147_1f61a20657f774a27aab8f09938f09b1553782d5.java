package com.example.a.myapplication;

import android.content.Context;
import android.util.Log;
import android.widget.Toast;

import com.android.volley.AuthFailureError;
import com.android.volley.Request;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.StringRequest;

import org.json.JSONException;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Speed on 13/04/2018.
 */

public class ObdUtils {
    private static final String TAG = "ObdUtils";
    private static Context mContext;
    private static ObdUtils mInstance;
    private ObdUtils(Context context){
        mContext=context;
    }
    public static ObdUtils getInstance(Context context){
        if(mInstance==null)
            mInstance=new ObdUtils(context);
        return mInstance;
    }

    public  void setObdData(final List<String>keys,final List<String>values){
        StringRequest stringRequest = new StringRequest(
                Request.Method.POST,
                Constants.URL_OBD,
                new Response.Listener<String>() {
                    @Override
                    public void onResponse(String response) {
                        Log.d(TAG, "onResponse: starts with response "+response);

                        try
                        {
                            JSONObject obj = new JSONObject(response);
                            Log.d(TAG, "onResponse: "+response);

                            if(!obj.getBoolean("error")){
                                Toast.makeText(mContext,"obd data inserted successfully", Toast.LENGTH_LONG).show();
                                Log.d(TAG, "onResponse: obd data inserted successfully");
                                //finish();
                            }else{
                                Toast.makeText(
                                        mContext,"error inserting OBD data"          ,
                                        Toast.LENGTH_LONG
                                ).show();
                                Log.d(TAG, "onResponse: error inserting OBD data+ "+obj.getString("message"));
                            }
                        } catch (JSONException e) {

                            Log.d(TAG, "onResponse: error"+response);
                            e.printStackTrace();
                        }
                    }
                },
                new Response.ErrorListener() {
                    @Override
                    public void onErrorResponse(VolleyError error) {
                        Log.d(TAG, "onErrorResponse: starts "+error.toString());

                        Toast.makeText(mContext, "VolleyError is  "+error.toString(), Toast.LENGTH_LONG).show();

                    }
                }
        ){
            @Override
            protected Map<String, String> getParams() throws AuthFailureError {
                String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());

                Log.d(TAG, "getParams: starts with time "+timeStamp);

                Map<String, String> params = new HashMap<>();
                params.put("submitObdData", "true");
                params.put("time", String.valueOf(timeStamp));
                params.put("userID", String.valueOf(SharedPrefManager.getInstance(mContext).getUserId()));
                params.put("carID", String.valueOf(SharedPrefManager.getInstance(mContext).getCarId()));

                if(keys.size()==values.size()) {
                    for (int i=0;i<keys.size();i++) {
                        params.put(keys.get(i), values.get(i));

                    }
                }else{
                    Log.d(TAG, "error keys != vals");
                }


                return params;
            }

        };

        RequestHandler.getInstance(mContext).addToRequestQueue(stringRequest);

    }
}