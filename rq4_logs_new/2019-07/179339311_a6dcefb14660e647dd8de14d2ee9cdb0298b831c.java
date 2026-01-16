package com.example.enigmator.controller;

import android.content.Context;
import android.os.AsyncTask;
import android.os.Environment;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import android.util.Log;

import com.example.enigmator.entity.Response;
import com.example.enigmator.entity.StoreMedia;

public class DownloadFileFromURL extends AsyncTask<StoreMedia, String, Response> {
    private static final String TAG = DownloadFileFromURL.class.getName();

    private static final String BASE_URL = "http://35.180.227.54:3000/api/Containers/enigme/download/";

    private final HttpRequest.HttpRequestListener listener;

    public DownloadFileFromURL(HttpRequest.HttpRequestListener listener) {
        this.listener = listener;
    }

    @Override
    protected void onPreExecute() {
        listener.prepareRequest();
    }

    @Override
    protected Response doInBackground(StoreMedia... mediaSpec) {
        Context context = mediaSpec[0].getContext();
        URL url;
        try {
            String urlText = BASE_URL + mediaSpec[0].getUrl();
            url = new URL(urlText);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }

        String fileName = mediaSpec[0].getUrl();
        String folderType;

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

            switch (mediaSpec[0].getMediaType().toLowerCase()) {
                case "video":
                    folderType = Environment.DIRECTORY_MOVIES;
                    break;
                case "image":
                    folderType = Environment.DIRECTORY_PICTURES;
                    break;
                case "audio":
                    folderType = Environment.DIRECTORY_MUSIC;
                    break;
                default:
                    folderType = Environment.DIRECTORY_DOWNLOADS;
            }
            OutputStream output = new FileOutputStream(
                    new File(context.getExternalFilesDir(folderType), fileName)
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
            Log.e(TAG + " Error: ", e.getMessage());
            return null;
        }
        return new Response(200,
                context.getExternalFilesDir(folderType) + "/" +fileName);
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