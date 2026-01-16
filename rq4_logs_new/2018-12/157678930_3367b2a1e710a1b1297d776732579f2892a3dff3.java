package wav.wav;


import android.content.pm.PackageManager;
import android.graphics.drawable.AnimationDrawable;
import android.os.Build;
import 	android.Manifest;

import android.os.Handler;
import android.os.Message;
import android.provider.MediaStore;
import android.support.v7.app.ActionBar;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import android.net.Uri;
import android.content.ContentResolver;
import android.database.Cursor;
import android.widget.Button;
import android.widget.ListView;


import android.os.IBinder;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.view.MenuItem;
import android.view.View;
import android.widget.SeekBar;
import android.widget.TextView;

import wav.wav.MusicService.MusicBinder;



public class Playing extends AppCompatActivity {

    protected ArrayList<Song> songList;
    protected ListView songView;

    protected MusicService musicSrv;
    protected Intent playIntent;
    protected boolean musicBound=false;


    protected boolean paused=false, playbackPaused=false;



    Button playBtn;
    Button loopBtn;
    SeekBar positionBar;
    SeekBar volumeBar;
    TextView elapsedTimeLabel;
    TextView remainingTimeLabel;
    AnimationDrawable ptp; //PlayToPause animation
    int totalTime;






    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.player);

    songList = getIntent().getParcelableArrayListExtra("songList");


        playBtn = (Button) findViewById(R.id.playBtn);
       // loopBtn= (Button) findViewById(R.id.loopBtn);
        elapsedTimeLabel = (TextView) findViewById(R.id.elapsedTimeLabel);
        remainingTimeLabel = (TextView) findViewById(R.id.remainingTimeLabel);


    }


    //connect to the service
    protected ServiceConnection musicConnection = new ServiceConnection(){

        @Override
        public void onServiceConnected(ComponentName name, IBinder service) {
            MusicBinder binder = (MusicBinder)service;
            //get service
            musicSrv = binder.getService();
            //pass list
            musicSrv.setList(songList);
            musicBound = true;


            playSong();

            //mp.setVolume(0.5f, 0.5f);
            totalTime = musicSrv.getDur();
            System.out.println("Totaltime: "+musicSrv.getDur());
            System.out.println("Posn: "+musicSrv.getPosn());



            // Position Bar
            positionBar = (SeekBar) findViewById(R.id.positionBar);
            positionBar.setMax(totalTime);
            System.out.println("MAX: "+positionBar.getMax());
            positionBar.setOnSeekBarChangeListener(
                    new SeekBar.OnSeekBarChangeListener() {
                        @Override
                        public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
                            if (fromUser) {
                                musicSrv.seek(progress);
                                positionBar.setProgress(progress);
                                System.out.println("Progress: "+progress);
                                System.out.println("POSN2: "+musicSrv.getPosn());

                            }
                        }

                        @Override
                        public void onStartTrackingTouch(SeekBar seekBar) {

                        }

                        @Override
                        public void onStopTrackingTouch(SeekBar seekBar) {

                        }
                    }
            );


            // Volume Bar
            volumeBar = (SeekBar) findViewById(R.id.volumeBar);
            volumeBar.setOnSeekBarChangeListener(
                    new SeekBar.OnSeekBarChangeListener() {
                        @Override
                        public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
                            float volumeNum = progress / 100f;
                            //mp.setVolume(volumeNum, volumeNum);
                        }

                        @Override
                        public void onStartTrackingTouch(SeekBar seekBar) {

                        }

                        @Override
                        public void onStopTrackingTouch(SeekBar seekBar) {

                        }
                    }
            );


            // Thread (Update positionBar & timeLabel)
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (musicSrv != null) {
                        try {
                            Message msg = new Message();
                            msg.what = musicSrv.getPosn();
                            handler.sendMessage(msg);
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {}
                    }
                }
            }).start();


        }






        @Override
        public void onServiceDisconnected(ComponentName name) {
            musicBound = false;
        }
    };




    private Handler handler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            int currentPosition = msg.what;
            // Update positionBar.
            positionBar.setProgress(currentPosition);

            // Update Labels.
            String elapsedTime = createTimeLabel(currentPosition);
            elapsedTimeLabel.setText(elapsedTime);

            String remainingTime = createTimeLabel(totalTime-currentPosition);
            remainingTimeLabel.setText("- " + remainingTime);
        }
    };

    public String createTimeLabel(int time) {
        String timeLabel = "";
        int min = time / 1000 / 60;
        int sec = time / 1000 % 60;

        timeLabel = min + ":";
        if (sec < 10) timeLabel += "0";
        timeLabel += sec;

        return timeLabel;
    }



    @Override
    protected void onStart() {
        super.onStart();
        if(playIntent==null){
            playIntent = new Intent(this, MusicService.class);
            bindService(playIntent, musicConnection, Context.BIND_AUTO_CREATE);
            startService(playIntent);
        }

    }


    public void playSong (){
        String s = getIntent().getStringExtra("SetSong");
        musicSrv.setSong(Integer.parseInt(s));
        musicSrv.playSong();

        playBtn.setBackgroundResource(R.drawable.stop);

        System.out.println("DURATION: "+musicSrv.getDur());


    }



    @Override
    protected void onDestroy() {
        stopService(playIntent);
        musicSrv=null;
        super.onDestroy();
    }



  //  @Override
   // public boolean onOptionsItemSelected(MenuItem item) {
     //   switch (item.getItemId()) {
       //     case R.id.loopBtn:
         //       musicSrv.setShuffle();
           //     break;
       // }
       // return false;
   // }


    @Override
    protected void onPause(){
        super.onPause();
        paused=true;
    }






    public void playBtnClick(View view) {

        if (!musicSrv.isPng()) {
            // Stopping


            playBtn.setBackgroundResource(R.drawable.playtopauseanim);
            ptp = (AnimationDrawable) playBtn.getBackground();

            ptp.start();
            musicSrv.go();




        } else {
            // Playing
            musicSrv.pausePlayer();
            playBtn.setBackgroundResource(R.drawable.play);

        }

    }






}