package com.example.enigmator.controller;

import android.content.Context;
import android.os.AsyncTask;
import android.os.Environment;
import android.util.Log;

import com.example.enigmator.entity.Response;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import static com.example.enigmator.controller.HttpRequest.HttpRequestListener;

public class DownloadProfilePictureTask extends AsyncTask<String, String, Response> {
    private static final String TAG = DownloadProfilePictureTask.class.getName();

    private static final String BASE_DOWNLOAD_URL = HttpAsyncTask.BASE_URL + "/Containers/profile/download/";

    private final HttpRequestListener listener;
    private final WeakReference<Context> weakContext;

    public DownloadProfilePictureTask(Context context, HttpRequestListener listener) {
        this.listener = listener;
        weakContext = new WeakReference<>(context);
    }

    @Override
    protected void onPreExecute() {
        listener.prepareRequest();
    }


    @Override
    protected Response doInBackground(String... params) {
        String fileName = params[0];
        URL url;
        try {
            String urlText = BASE_DOWNLOAD_URL + params[0];
            url = new URL(urlText);
        } catch (MalformedURLException e) {
            Log.e(TAG, "MalformedURL: " + BASE_DOWNLOAD_URL + fileName);
            cancel(false);
            return new Response(400, e.getMessage());
        }

        try {
            URLConnection connection = url.openConnection();
            connection.connect();

            // this will be useful so that you can show a tipical 0-100%
            List values = connection.getHeaderFields().get("content-Length");
            int length;
            if (values != null && !values.isEmpty()) length = (int) values.get(0);
            else length = 8096;


            InputStream input = new BufferedInputStream(url.openStream(),
                    length);

            OutputStream output = new FileOutputStream(
                    new File(weakContext.get().getExternalFilesDir(Environment.DIRECTORY_PICTURES), fileName)
            );

            byte[] data = new byte[1024];

            int count;
            while ((count = input.read(data)) != -1) {
                output.write(data, 0, count);
            }

            // flushing output
            output.flush();

            // closing streams
            output.close();
            input.close();
        } catch (Exception e) {
            Log.e(TAG, "Error: " + e.getMessage());
            return new Response(400, null);
        }
        return new Response(200,
                weakContext.get().getExternalFilesDir(Environment.DIRECTORY_PICTURES) + "/" +fileName);

    }

    @Override
    protected void onPostExecute(Response response) {
        listener.handleSuccess(response);
    }

    @Override
    protected void onCancelled(Response response) {
        listener.handleError(response);
    }
}