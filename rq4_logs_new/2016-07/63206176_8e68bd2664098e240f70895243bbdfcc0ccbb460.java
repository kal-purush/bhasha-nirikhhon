package com.example.alex.realtest2;

import android.app.Service;
import android.content.ContentUris;
import android.content.Intent;
import android.media.AudioManager;
import android.media.MediaPlayer;
import android.net.Uri;
import android.os.Binder;
import android.os.IBinder;
import android.os.PowerManager;
import android.provider.MediaStore;
import android.support.annotation.Nullable;
import android.widget.MediaController;

import java.util.ArrayList;

/**
 * Created by Alex on 7/14/2016.
 */

public class MP3_Service extends Service implements MediaPlayer.OnPreparedListener,MediaPlayer.OnErrorListener,MediaPlayer.OnCompletionListener {
    private MediaPlayer mp3Player;//does the media playing
    private ArrayList<Song> mp3SongList;//once again we need the songs on the device
    private int trackNumber;//which song are we on? this keeps track of that
    private final IBinder mp3ServiceBinder= new mp3ServiceBinder();
    public void onCreate(){
        super.onCreate();//create the service
        trackNumber=0;
        mp3Player= new MediaPlayer();

        initMp3Player();

    }

    //sets up our media player
    public void initMp3Player(){
        mp3Player.setWakeMode(getApplicationContext(), PowerManager.PARTIAL_WAKE_LOCK);//works while device sleeps
        mp3Player.setAudioStreamType(AudioManager.STREAM_MUSIC);//tells it to play music basically

        mp3Player.setOnPreparedListener(this);//"this" being an onpreparedllistener. we have one cuz we implemented it. see methods below.
        mp3Player.setOnCompletionListener(this);
        mp3Player.setOnErrorListener(this);
    }

    //allows us to tell the service what songs wed like to use, from anywhere.
    public void setMp3SongList(ArrayList<Song> songs){
        this.mp3SongList=songs;
    }



    //part of the service superclass
    //returns a binder, which in turn holds the service for us and lets us bind to it
    //we must show what binder represents this object when onBind is called.
    @Nullable
    @Override
    public IBinder onBind(Intent intent)
    {
        return mp3ServiceBinder;
    }

    //when the service is UNbinded, we should stop all playback and such
    public boolean onUnbind(Intent intent){
        mp3Player.stop();//stop playback
        mp3Player.release();//release resources
        return false;//idk why
    }



    //the interface stuff here will work with the mediaplayer
    //TODO: will change for other classes
    //happens when the media finishes playing. i.e., the song ends for any reason
    @Override
    public void onCompletion(MediaPlayer mediaPlayer) {
        if(mp3Player.getCurrentPosition()>0){
            mp3Player.reset();
            playNextTrack();
        }
    }

    @Override
    public boolean onError(MediaPlayer mediaPlayer, int i, int i1) {
        mp3Player.reset();
        return false;
    }

    @Override
    //runs when .prepareAsync() is called
    public void onPrepared(MediaPlayer mediaPlayer) {
        mediaPlayer.start();
    }

    //some playback methods
    public void playSong(){
        mp3Player.reset();//have to reset since it will be used many times
        Song songToPlay=mp3SongList.get(trackNumber);//pick the song according to the track number we're on.
        //when we wish to play a track, we set the track number (in a different method) and hit play.

        long idOfDesiredSong=songToPlay.getId();//use our getter method to get the id
        //
        //set uri "address" of the song file. we search in the MediaStore and look for a song with the ID
        Uri trackUri= ContentUris.withAppendedId(MediaStore.Audio.Media.EXTERNAL_CONTENT_URI,idOfDesiredSong);

        try{
            mp3Player.setDataSource(getApplicationContext(),trackUri);//the player is gonna play this file
        }
        catch (Exception ex){
            //whatevs
        }

        mp3Player.prepareAsync();//now the mp3 player will play. this calls other methods that will play the track.
        //this will run onPrepared().
    }

    //setter methods so other classes may set which track to play.
    //naturally this doesnt automatically start playing the song.
    public void setTrackNumber(int number){
        this.trackNumber=number;
    }


    //returns mp3 stuff. the main activity needs to see this stuff, so the service acts as an intermediary.
    //hence why it seems to return the same stuff
    public int getCurrentPosition(){
        return mp3Player.getCurrentPosition();
    }
    public int getDuration(){
        return mp3Player.getDuration();
    }
    public boolean isPlaying(){
        return mp3Player.isPlaying();
    }
    public void pausePlayback(){
        mp3Player.pause();
    }
    public void seekTo(int position){
        mp3Player.seekTo(position);
    }
    public void startPlayback(){
        mp3Player.start();
    }
    public void playPreviousTrack(){
        trackNumber--;
        if(trackNumber<0){
            trackNumber=mp3SongList.size()-1;
        }
        playSong();
    }
    public void playNextTrack(){
        trackNumber++;
        if(trackNumber>=mp3SongList.size()){
            trackNumber=0;
        }
        playSong();
    }





    public class mp3ServiceBinder extends Binder {
        //returns this service. this binder class acts as like a wrapper.
        MP3_Service getService(){
            return MP3_Service.this;
        }

    }
}