package com.example.eventmanageri.Activities;


import androidx.appcompat.app.AppCompatActivity;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.widget.Button;
import android.widget.MediaController;
import android.widget.VideoView;

import com.example.eventmanageri.R;


public class VideoActivity extends AppCompatActivity {
    private static final String TAG = "VideoActivity";

    // UI reference
    private MediaController mediaC;
    private VideoView mVideoView;
    private Button mBtnChoose;

    // Data reference
    private String key, userid, email, title, date, type, memo, photo, video, location, share;

    // Activity reference
    public interface ActivityConstants {
        int ACTIVITY_1 = 1001;  // NewEventActivity
        int ACTIVITY_2 = 1002;  // EventModActivity
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_video);

        // Define UI reference
        mVideoView = (VideoView) findViewById(R.id.videoView2);
        if (video != null) {
            mVideoView.setVideoURI(Uri.parse(video));

            String path = "android.resource://" + getPackageName() + "/" + R.raw.cloud;
            Uri u = Uri.parse(path);
            mVideoView.setVideoURI(u);
            mVideoView.setMediaController(mediaC);
            mediaC.setAnchorView(mVideoView);
            mVideoView.start();


            // Get data from the calling activity
            key = getIntent().getStringExtra("key");
            userid = getIntent().getStringExtra("userid");
            email = getIntent().getStringExtra("email");
            title = getIntent().getStringExtra("title");
            memo = getIntent().getStringExtra("memo");
            photo = getIntent().getStringExtra("photo");
            video = getIntent().getStringExtra("video");
            location = getIntent().getStringExtra("location");
            type = getIntent().getStringExtra("type");
            share = getIntent().getStringExtra("share");
            Log.d("CalendarActivity", "sss View key value (photo - calendar 222):" + photo);
        }
    }
}


