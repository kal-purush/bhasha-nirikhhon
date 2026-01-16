package com.clubjevin.festivus;

import android.media.AudioManager;
import android.media.MediaPlayer;
import android.util.Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by kevin on 12/8/16.
 */

public class MediaFilePlayer {
    MediaPlayer mediaPlayer;

    public MediaFilePlayer() {
        mediaPlayer = null;
    }

    public synchronized void play(File file) {
        if(file == null) {
            Log.v("media", "Attempted to play a null file");
            return;
        }

        if(!file.exists()) {
            Log.v("media", "Attempted to play a file that does not exist: " + file.getAbsolutePath());
            return;
        }

        boolean isPlaying = false;
        try {
            if(mediaPlayer != null) {
                isPlaying = mediaPlayer.isPlaying();
            }
        } catch(IllegalStateException e) {
            Log.v("media", "definitely not playing: IllegalStateException");
        }

        if(isPlaying) {
            Log.v("media", "Ignoring play request: already playing");
            return;
        }

        mediaPlayer = new MediaPlayer();
        Log.v("media", "Created media player");
        mediaPlayer.setAudioStreamType(AudioManager.STREAM_MUSIC);
        Log.v("media", "Set STREAM_MUSIC");
        try {
            FileInputStream fis = new FileInputStream(file);
            mediaPlayer.setDataSource(fis.getFD());
            mediaPlayer.prepare();
            mediaPlayer.start();
            Log.v("media", "Playing file: " + file.getAbsolutePath());
        } catch (IOException e) {
            Log.v("media", "Failed to play: " + e.getStackTrace().toString());
            e.printStackTrace();
        }

        mediaPlayer.setOnCompletionListener(new MediaPlayer.OnCompletionListener() {
            @Override
            public void onCompletion(MediaPlayer mediaPlayer) {
                Log.v("media", "Player complete");
                mediaPlayer.stop();
                mediaPlayer.release();
            }
        });
    }
}