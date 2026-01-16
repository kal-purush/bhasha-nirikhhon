package com.example.todd.ad340;

import android.content.Context;
import android.media.AudioManager;
import android.media.MediaPlayer;
import android.os.Environment;
import android.support.v7.app.ActionBarActivity;
import android.support.v7.app.ActionBar;
import android.support.v4.app.Fragment;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.os.Build;

import java.io.IOException;


public class APBTutorial extends ActionBarActivity {

    private Context mContext;
    private AudioManager mAudioManager;
    private int mAudioManagerResult;
    private MediaPlayer mMediaPlayer;
    private AudioManager.OnAudioFocusChangeListener mAudioListener;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_apbtutorial);
        if (savedInstanceState == null) {
            getSupportFragmentManager().beginTransaction()
                    .add(R.id.container, new PlaceholderFragment())
                    .commit();
        }
        mContext = getApplicationContext();

        mAudioListener = new AudioManager.OnAudioFocusChangeListener(){
            public void onAudioFocusChange(int focusChange) {
                switch(focusChange)
                {
                    case AudioManager.AUDIOFOCUS_LOSS_TRANSIENT:
                        //LOWER VOLUME
                        mMediaPlayer.setVolume(0.0f, 0.0f);
                        break;
                    case AudioManager.AUDIOFOCUS_GAIN:
                        //PLAY MP3 RESOURCE
                        // resume playback
                        if (mMediaPlayer == null) initMediaPlayer();
                        else if (!mMediaPlayer.isPlaying()) mMediaPlayer.start();
                        mMediaPlayer.setVolume(1.0f, 1.0f);
                        break;
                    case AudioManager.AUDIOFOCUS_LOSS:
                        //STOP MP3
                        mMediaPlayer.stop();
                        break;
                }
            }
        };
        setVolumeControlStream(AudioManager.STREAM_MUSIC);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_apbtutorial, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    /**
     * A placeholder fragment containing a simple view.
     */
    public static class PlaceholderFragment extends Fragment {

        public PlaceholderFragment() {
        }

        @Override
        public View onCreateView(LayoutInflater inflater, ViewGroup container,
                                 Bundle savedInstanceState) {
            View rootView = inflater.inflate(R.layout.fragment_apbtutorial, container, false);
            return rootView;
        }
    }

    public void echo_click(View view) {
        mAudioManager = (AudioManager) mContext.getSystemService(Context.AUDIO_SERVICE);
        mAudioManagerResult = mAudioManager.requestAudioFocus(
                mAudioListener,
                AudioManager.STREAM_MUSIC,
                AudioManager.AUDIOFOCUS_GAIN);

        if(mAudioManagerResult ==
                AudioManager.AUDIOFOCUS_REQUEST_GRANTED)
        {
            //mAudioManager.registerMediaButtonEventReceiver(RemoteControlReceiver);
            // Start playback.
            //mMediaPlayer.start();
        }
    }

    private void initMediaPlayer()
    {
        int resId = getResources().getIdentifier("echo", "raw", getPackageName());
        mMediaPlayer = MediaPlayer.create(this, resId);
        mMediaPlayer.setAudioStreamType(AudioManager.STREAM_MUSIC);
        try {
            mMediaPlayer.prepare();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}