package com.caregiver.CustomWindowAdapter;

import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.RatingBar;
import android.widget.TextView;

import com.caregiver.Model.Caregiver;
import com.caregiver.R;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.model.Marker;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Query;

import java.util.HashMap;

/**
 * Created by Demos on 1/29/2018.
 */

public class CustomWindowAdapter implements GoogleMap.InfoWindowAdapter {

    LayoutInflater mInflater;
    private HashMap<Marker, Caregiver> caregiverDB;
    private HashMap<Marker, Float> score;
    private HashMap<Marker, Integer> review_num;
    public CustomWindowAdapter(LayoutInflater i ,HashMap<Marker, Caregiver> caregiverDB){
        this.caregiverDB = caregiverDB;
        /*this.score = score;
        this.review_num = review_num;*/
        mInflater = i;
    }

    @Override
    public View getInfoWindow(Marker marker) {
        return null;
    }

    @Override
    public View getInfoContents(Marker marker) {
        View v = mInflater.inflate(R.layout.custom_info_window, null);
        if(!marker.getId().equalsIgnoreCase("m0")){

            TextView title = (TextView) v.findViewById(R.id.custom_info_window_name);
            title.setText(caregiverDB.get(marker).getName()+" "+caregiverDB.get(marker).getLastname());

            TextView job = (TextView) v.findViewById(R.id.custom_info_window_job);
            job.setText(caregiverDB.get(marker).getJob());

            TextView cost = (TextView) v.findViewById(R.id.custom_info_window_cost);
            cost.setText(caregiverDB.get(marker).getCost()+" บาท/ครั้ง");
            return v;
        }
        Log.d(marker.getId(), "getInfoContents: ");




        /*TextView numreview = (TextView) v.findViewById(R.id.custom_info_window_numreview);
        numreview.setText(review_num.get(marker));

        RatingBar ratingbar = v.findViewById(R.id.custom_info_window_ratingbar);
        ratingbar.setRating(score.get(marker));*/


        return null;
    }
}