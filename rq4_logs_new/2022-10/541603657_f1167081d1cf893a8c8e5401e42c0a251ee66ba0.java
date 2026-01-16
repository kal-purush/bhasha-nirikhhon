package com.aptech.team5.mysound.Activity;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.viewpager.widget.ViewPager;

import android.content.Intent;
import android.graphics.Color;
import android.media.AudioManager;
import android.media.MediaPlayer;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Handler;
import android.os.StrictMode;
import android.view.View;
import android.widget.ImageButton;
import android.widget.SeekBar;
import android.widget.TextView;

import com.aptech.team5.mysound.Adapter.ViewPagerPlaylistsong;
import com.aptech.team5.mysound.Fragment.Fragment_Disc;
import com.aptech.team5.mysound.Fragment.Fragment_Play_Song_List;
import com.aptech.team5.mysound.Model.Song;
import com.aptech.team5.mysound.R;
import com.squareup.picasso.Picasso;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import de.hdodenhof.circleimageview.CircleImageView;

public class PlaySongActivity extends AppCompatActivity {
    Toolbar toolbarplaysong;
    TextView txtTimesong,txtTotaltimesong;
    SeekBar seektime;
    ImageButton imgplay,imgrepeat,imgnext,imgpre,imgrandom;
    ViewPager viewPagerplaysong;
    Fragment_Disc fragment_disc;
    Fragment_Play_Song_List fragment_play_song_list;
    public static ArrayList<Song> arraySong = new ArrayList<>();
    public static ViewPagerPlaylistsong adaptersong;
    MediaPlayer mediaPlayer;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_play_song);
        StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
        StrictMode.setThreadPolicy(policy);
        GetDataFromIntent();
        Init();
        eventClick();
    }

    private void eventClick() {
        final Handler handler = new Handler();
        handler.postDelayed(new Runnable() {
            @Override
            public void run() {
                if(adaptersong.getItem(1) != null) {
                    if(arraySong.size() > 0) {
                        fragment_disc.PlaySong(arraySong.get(0).getImageSong());
                        handler.removeCallbacks(this);
                    }else  {
                        handler.postDelayed(this,300);
                    }
                }
            }
        },500);
        imgplay.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                if(mediaPlayer.isPlaying()){
                    mediaPlayer.pause();
                    imgplay.setImageResource(R.drawable.play);
                }else {
                    mediaPlayer.start();
                    imgplay.setImageResource(R.drawable.pause);
                }
            }
        });
    }

    private void GetDataFromIntent() {
        Intent intent = getIntent();
        arraySong.clear();
        if(intent != null){
            if(intent.hasExtra("song")){
                Song song = intent.getParcelableExtra("song");
                arraySong.add(song);
            }

            if (intent.hasExtra("listsong")) {
                ArrayList<Song> songArrayList = intent.getParcelableArrayListExtra("listsong");
                arraySong = songArrayList;
            }
        }

    }

    private void Init() {
        toolbarplaysong = findViewById(R.id.toolbarplaysong);
        txtTimesong = findViewById(R.id.textviewtimesong);
        txtTotaltimesong = findViewById(R.id.textviewtotaltimesong);
        seektime = findViewById(R.id.seekbarsong);
        imgplay = findViewById(R.id.imagebuttonplay);
        imgrepeat = findViewById(R.id.imagebuttonrepeat);
        imgnext = findViewById(R.id.imagebuttonnext);
        imgrandom = findViewById(R.id.imagebuttonsuffle);
        imgpre = findViewById(R.id.imagebuttonpre);
        viewPagerplaysong = findViewById(R.id.viewpagerplaysong);
        setSupportActionBar(toolbarplaysong);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        toolbarplaysong.setNavigationOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });
        toolbarplaysong.setTitleTextColor(Color.WHITE);
        fragment_disc = new Fragment_Disc();
        fragment_play_song_list = new Fragment_Play_Song_List();
        adaptersong = new ViewPagerPlaylistsong(getSupportFragmentManager());
        adaptersong.AddFragment(fragment_play_song_list);
        adaptersong.AddFragment(fragment_disc);
        viewPagerplaysong.setAdapter(adaptersong);
        fragment_disc = (Fragment_Disc) adaptersong.getItem(1);
        if(arraySong.size() > 0){
            getSupportActionBar().setTitle(arraySong.get(0).getNameSong());
            new PlayMp3().execute(arraySong.get(0).getLinkSong());
            imgplay.setImageResource(R.drawable.pause);
        }
    }

    class PlayMp3 extends AsyncTask<String,Void,String>{

        @Override
        protected String doInBackground(String... strings) {
            return strings[0];
        }

        @Override
        protected void onPostExecute(String song) {
            super.onPostExecute(song);
            try {
            mediaPlayer = new MediaPlayer();
            mediaPlayer.setAudioStreamType(AudioManager.STREAM_MUSIC);
            mediaPlayer.setOnCompletionListener(new MediaPlayer.OnCompletionListener() {
                @Override
                public void onCompletion(MediaPlayer mp) {
                    mp.stop();
                    mp.reset();
                }
            });
            mediaPlayer.setDataSource(song);
            mediaPlayer.prepare();
            } catch (IOException e) {
                e.printStackTrace();
            }
            mediaPlayer.start();
            TimeSong();
        }
    }

    private void TimeSong() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("mm:ss");
        txtTotaltimesong.setText(simpleDateFormat.format(mediaPlayer.getDuration()));
        seektime.setMax(mediaPlayer.getDuration());
    }
}