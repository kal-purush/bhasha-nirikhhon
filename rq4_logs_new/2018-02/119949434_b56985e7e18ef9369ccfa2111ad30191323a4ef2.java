package com.kth.barfinder.barfinder_native;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.AsyncTask;
import android.util.Log;


import com.google.android.gms.maps.CameraUpdateFactory;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.model.BitmapDescriptorFactory;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.Marker;
import com.google.android.gms.maps.model.MarkerOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Shayan on 2018-02-07.
 */

public class GetPictures extends AsyncTask<Object, String, String> {

    private String image_data;  // whatever data type the image is
    private GoogleMap mMap;
    private String url;
    private Object infoDataObject;
    private InfoWindowData infoDataList;
    private int markerCounter;




    @Override
    protected String doInBackground(Object... params) {
        try {
            mMap = (GoogleMap) params[0];
            url = (String) params[1];
            infoDataObject= params[2];
            infoDataList = (InfoWindowData) infoDataObject;

            DownloadUrl downloadUrl = new DownloadUrl();
            image_data = downloadUrl.readUrl(url);
        } catch (Exception e) {
            Log.d("GooglePlacesReadTask", e.toString());
        }
        return image_data;
    }

    @Override
    protected void onPostExecute(String image) {
        //List<HashMap<String, String>>  = null;
        //DataParser dataParser = new DataParser();
        //nearbyPlacesList = dataParser.parse(result);
        setNearbyMarkers(image);
    }

    private void setNearbyMarkers(String image) {

        infoDataList.setImage("beermugsmall"); // here goes the image
        //infoDataList.setPrice("testen");



    }



}