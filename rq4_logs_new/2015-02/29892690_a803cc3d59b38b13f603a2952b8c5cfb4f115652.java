package com.quiztads.ufpr.br.quiztads;

import android.util.Log;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

/**
 * Created by DIULLIAN on 14/02/2015.
 */
public class WebService {

    DefaultHttpClient httpClient;
    HttpContext localContext;
    private String ret;

    HttpResponse response = null;
    HttpPost httpPost = null;
    HttpGet httpGet = null;
    String webServiceUrl;

    //The serviceName should be the name of the Service you are going to be using.
    public WebService(String serviceName){
        HttpParams myParams = new BasicHttpParams();

        HttpConnectionParams.setConnectionTimeout(myParams, 10000);
        HttpConnectionParams.setSoTimeout(myParams, 10000);
        httpClient = new DefaultHttpClient(myParams);
        localContext = new BasicHttpContext();
        webServiceUrl = serviceName;
    }

    //Use this method to do a HttpGet/WebGet on the web service
    public String webGet(String methodName, Map<String, String> params) {
        String getUrl = webServiceUrl + methodName;

        int i = 0;
        for (Map.Entry<String, String> param : params.entrySet())
        {
            if(i == 0){
                getUrl += "?";
            }
            else{
                getUrl += "&";
            }

            try {
                getUrl += param.getKey() + "=" + URLEncoder.encode(param.getValue(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            i++;
        }

        httpGet = new HttpGet(getUrl);
        Log.e("WebGetURL: ", getUrl);

        try {
            response = httpClient.execute(httpGet);
        } catch (Exception e) {
            Log.e("Groshie:", e.getMessage());
        }

        // we assume that the response body contains the error message
        try {
            ret = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            Log.e("Groshie:", e.getMessage());
        }

        return ret;
    }

    public void clearCookies() {
        httpClient.getCookieStore().clear();
    }

    public void abort() {
        try {
            if (httpClient != null) {
                System.out.println("Abort.");
                httpPost.abort();
            }
        } catch (Exception e) {
            System.out.println("Quiz Tads excpetion! = " + e);
        }
    }

}