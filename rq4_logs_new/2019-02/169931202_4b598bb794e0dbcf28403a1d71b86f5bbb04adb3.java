package com.example.amine.learn2sign;

import android.os.AsyncTask;
import android.widget.RadioButton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cz.msebera.android.httpclient.HttpResponse;
import cz.msebera.android.httpclient.NameValuePair;
import cz.msebera.android.httpclient.client.ClientProtocolException;
import cz.msebera.android.httpclient.client.HttpClient;
import cz.msebera.android.httpclient.client.entity.UrlEncodedFormEntity;
import cz.msebera.android.httpclient.client.methods.HttpPost;
import cz.msebera.android.httpclient.impl.client.DefaultHttpClient;
import cz.msebera.android.httpclient.message.BasicNameValuePair;
import cz.msebera.android.httpclient.util.EntityUtils;

public class PostData extends AsyncTask< String, Void, String > {
    String uri;
    String count;
    RadioButton rb;
    public PostData(String uri)
    {
        this.uri = uri;

    }
    protected String doInBackground(String...params) {

        // Create a new HttpClient and Post Header
        HttpClient httpclient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost(this.uri);

        try {
            // Add your data
            String id =params[0];

            List<NameValuePair> nameValuePairs = new ArrayList< NameValuePair >();
            nameValuePairs.add(new BasicNameValuePair("id", id));

            httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs));

            // Execute HTTP Post Request
            HttpResponse response = httpclient.execute(httppost);

            this.count= EntityUtils.toString(response.getEntity());
            System.out.println("-------------response--------------------  " + this.count);

        } catch (ClientProtocolException e) {
            // TODO Auto-generated catch block
        } catch (IOException e) {
            // TODO Auto-generated catch block
        }
        return this.count;
    }

    @Override
    protected void onPostExecute(String result) {
        super.onPostExecute(result);
        System.out.println("onPostExecute" + this.count);
//        MainActivity.setVideoCount(this.count.trim() ,MainActivity.class);
          processResult(result);
//        rb.setEnabled(true);
//        ma.rb_practice.setEnabled(true);

    }
    public String processResult(String result)
    {
        return result;
    }

}