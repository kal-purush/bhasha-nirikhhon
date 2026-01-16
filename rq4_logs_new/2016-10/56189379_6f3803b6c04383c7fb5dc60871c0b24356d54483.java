package com.canthonyscott.microinjectioncalc;

import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;

import javax.net.ssl.HttpsURLConnection;

/**
 * Created by Anthony on 10/13/2016.
 */

public class APIComm {

    String charset = "UTF-8";

    public APIComm() {
    }


    public String makeHttpsRequestPOST(String url, HashMap<String, String> params){

        String base_url = "https://injectioncalculatorapi.appspot.com";
        String specificUrl = url;
        String wholeUrl = base_url + specificUrl;
        HttpsURLConnection conn;
        StringBuilder result = null;

        // SEND POST REQUEST TO SERVER
        try{
            Log.d("APIComm", "whole url: " + wholeUrl);
            URL InjCalcAPI = new URL(wholeUrl);
            conn = (HttpsURLConnection) InjCalcAPI.openConnection();
            conn.setRequestMethod("POST");
//            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept-Charset", charset);
            conn.setDoOutput(true);
            conn.connect();
            DataOutputStream wr = new DataOutputStream(conn.getOutputStream());

            String paramsString = paramsToString(params);
            wr.writeBytes(paramsString);
            wr.flush();
            wr.close();

        } catch (Exception e){
            e.printStackTrace();
            return "failed";
        }

        // RECIEVE THE RESPONSE FROM THE SERVER
        try{
            InputStream in = new BufferedInputStream(conn.getInputStream());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            result = new StringBuilder();
            String line;
            while((line = reader.readLine()) != null){
                result.append(line);
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        try{
            int responseCode = conn.getResponseCode();
            Log.d("APIComm", "Response Code: " + responseCode);
        } catch (IOException e){
            e.printStackTrace();
        }
        conn.disconnect();
        return result.toString();
    }

    private String paramsToString(HashMap<String,String> params){
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String key : params.keySet()){
            try{
                if (i != 0){
                    sb.append("&");
                }
                sb.append(key).append("=")
                        .append(URLEncoder.encode(params.get(key), charset));
            } catch (UnsupportedEncodingException e){
                e.printStackTrace();
            }
            i++;
        }

        return sb.toString();
    }



}