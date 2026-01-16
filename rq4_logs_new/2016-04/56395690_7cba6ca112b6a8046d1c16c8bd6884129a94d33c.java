package com.example.wansu.loginapp;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.os.AsyncTask;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Created by wansu on 20/04/16.
 */
public class BackgroundTask extends AsyncTask <String,Void,String> {
    String register_url = "http://localhost/loginapp/register.php";
    Context ctx;
    Activity activity;
    AlertDialog.Builder builder;

    //constructor
    public BackgroundTask(Context ctx)
    {
        this.ctx = ctx;
        activity = (Activity)ctx;
    }

    @Override
    protected void onPreExecute() {
        builder = new AlertDialog.Builder(ctx);
    }

    @Override //http://www.codeproject.com/Tips/773464/How-to-Add-an-AsyncTask-to-an-Android-Activity
    protected String doInBackground(String... params) {
        String method = params[0];
        if(method.equals("register"))
        {
            try
            {
                URL url = new URL(register_url);
                HttpURLConnection httpURLConnection = (HttpURLConnection)url.openConnection();
                httpURLConnection.setRequestMethod("POST");
                httpURLConnection.setDoOutput(true);
                httpURLConnection.setDoInput(true);
                OutputStream outputStream = httpURLConnection.getOutputStream();
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream,"UTF-8"));
                String name = params[1];
                String email = params[2];
                String pass = params[3];
                String data = URLEncoder.encode("name","UTF-8")+"="+URLEncoder.encode(name,"UTF-8")+"&"+
                        URLEncoder.encode("email","UTF-8")+"="+URLEncoder.encode(email,"UTF-8")+"&"+
                        URLEncoder.encode("pass","UTF-8")+"="+URLEncoder.encode(pass,"UTF-8");
                bufferedWriter.write(data);
                bufferedWriter.flush();
                bufferedWriter.close();;
                outputStream.close();
                InputStream inputStream = httpURLConnection.getInputStream();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                StringBuilder stringBuilder = new StringBuilder();
                String line = "";
                while ((line=bufferedReader.readLine())!=null)
                {
                    stringBuilder.append(line+"\n");
                }

                httpURLConnection.disconnect();;
                return stringBuilder.toString().trim();

            }
            catch (MalformedURLException e)
            {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    protected void onProgressUpdate(Void... values) {
        super.onProgressUpdate(values);
    }

    @Override
    protected void onPostExecute(String json)
    {
        try
        {
            JSONObject jsonObject = new JSONObject(json);
            JSONArray jsonArray = jsonObject.getJSONArray("server_response");
            JSONObject JO = jsonArray.getJSONObject(0);
            String code = JO.getString("code");
            String message = JO.getString("message");
            if(code.equals("reg_true"))
            {
                showDialog("Registration Success"), message, code);
            }
            else if(code.equals("reg_false"))
            {
                showDialog("Registration Failed"), message, code);
            }
        }
        catch (JSONException e)
        {
            e.printStackTrace();
        }
    }
    public void showDialog(String title, String message, String code)
    {
        builder.setTitle(title);
        if(code.equals("reg_true")||code.equals("reg_false"))
        {
            builder.setMessage(message);
            builder.setPositiveButton("OK")
        }
    }
}
