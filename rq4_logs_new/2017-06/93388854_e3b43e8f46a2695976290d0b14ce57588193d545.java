package com.utsavmobileapp.utsavapp.fetch;

import android.content.Context;
import android.content.Intent;
import android.os.AsyncTask;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.TextView;

import com.utsavmobileapp.utsavapp.DetailsActivity;
import com.utsavmobileapp.utsavapp.R;
import com.utsavmobileapp.utsavapp.parser.ParseCheckinJSON;
import com.utsavmobileapp.utsavapp.service.Common;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Bibaswann on 10-06-2017.
 */

public class FetchCheckin extends AsyncTask<Void, Void, Void> {

    Context mContext;
    LinearLayout containerLayout;
    String mLat, mLon;
    ProgressBar progress;
    String pageNum, limNum;
    String uid;

    com.utsavmobileapp.utsavapp.service.Common Common;
    private List<String> fName = new ArrayList<>();
    private List<String> fId = new ArrayList<>();
    private List<String> fAddress = new ArrayList<>();
    private List<String> fDistance = new ArrayList<>();
    private List<String> fRating = new ArrayList<>();
    private List<String> fImg = new ArrayList<>();
    private List<String> lat = new ArrayList<>();
    private List<String> lon = new ArrayList<>();

    public FetchCheckin(Context context, LinearLayout holderLayout, ProgressBar prog, String page, String lim, String lat, String lon, String userId) {
        mContext = context;
        containerLayout = holderLayout;
        progress = prog;
        pageNum = page;
        limNum = lim;
        mLat = lat;
        mLon = lon;
        Common = new Common(mContext);
        uid = userId;
    }

    @Override
    protected void onPreExecute() {
        progress.setVisibility(View.VISIBLE);
        super.onPreExecute();
    }

    @Override
    protected Void doInBackground(Void... params) {
        ParseCheckinJSON prnfj;
        String fetchUrl;
        fetchUrl = mContext.getString(R.string.uniurl) + "/api/festival.php?type=USERCHCEKIN&lat=" + mLat + "&lng=" + mLon + "&user_id=" + uid + "&page=" + pageNum + "&limit=" + limNum;

//        Log.e("important","url of sponsored is "+fetchUrl);
        prnfj = new ParseCheckinJSON(fetchUrl, mContext);
        prnfj.fetchJSON();
        while (prnfj.parsingInComplete && (!this.isCancelled())) ;
        fName = prnfj.getfName();
        fId = prnfj.getfId();
        lat = prnfj.getLat();
        lon = prnfj.getLon();
        fAddress = prnfj.getfAddress();
        fDistance = prnfj.getfDistance();
        fRating = prnfj.getfRating();
        fImg = prnfj.getfImg();
//        Log.e("important", "found "+fId.size()+" items");
        return null;
    }

    @Override
    protected void onPostExecute(Void aVoid) {
        progress.setVisibility(View.GONE);
        String latlong = "";

        for (final String id : fId) {
            final int index = fId.indexOf(id);
            LinearLayout ll = (LinearLayout) LayoutInflater.from(mContext).inflate(R.layout.festival_block, containerLayout, false);

            //Log.e("important","image "+fImg.get(index)+" and rating "+fRating.get(index));
            latlong = latlong + "|" + lat.get(index) + "," + lon.get(index);
            ImageView img = (ImageView) ll.findViewById(R.id.imageFestival);
            Common.ImageDownloaderTask(img, mContext, fImg.get(index), "festival");

            TextView name = (TextView) ll.findViewById(R.id.textView6);
            name.setText(fName.get(index));

            TextView addr = (TextView) ll.findViewById(R.id.textView8);
            addr.setText(fAddress.get(index));

            TextView dist = (TextView) ll.findViewById(R.id.textView17);
            dist.setText(fDistance.get(index));

            TextView rat = (TextView) ll.findViewById(R.id.textView24);
            rat.setText(fRating.get(index));

            ll.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    Intent i = new Intent(mContext, DetailsActivity.class);
                    i.putExtra("id", id);
                    i.putExtra("name", fName.get(index));
                    mContext.startActivity(i);
                }
            });


            containerLayout.addView(ll);


        }


        super.onPostExecute(aVoid);
    }


}