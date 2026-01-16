package com.sun.music_64.mediaplayer;

import android.media.MediaPlayer;
import android.net.Uri;

import com.sun.music_64.data.model.Track;
import com.sun.music_64.service.PlayMusicService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PlayMediaManager extends PlayMusicSetting
        implements PlayMusicInterface {
    public static PlayMediaManager sIntance;
    private MediaPlayer mMediaPlayer;
    private PlayMusicService mPlayMusicService;
    private List<Track> mTracks;
    private Track mCurrentTrack;

    public PlayMediaManager(PlayMusicService playMusicService) {
        mTracks = new ArrayList<>();
        mMediaPlayer = new MediaPlayer();
        mLoopType = LoopType.ZERO;
        mShuffleType = ShuffleType.OFF;
        mPlayMusicService = playMusicService;
    }

    public static PlayMediaManager getIntance(PlayMusicService playMusicService) {
        if (sIntance == null) {
            sIntance = new PlayMediaManager(playMusicService);
        }
        return sIntance;
    }

    @Override
    public void create(Track track) {
        mMediaPlayer.reset();
        try {
            mMediaPlayer.setDataSource(mPlayMusicService, Uri.parse(track.getStreamUrl()));
        } catch (IOException e) {
            mPlayMusicService.onFailure();
        }
        mMediaPlayer.setOnErrorListener(mPlayMusicService);
        mMediaPlayer.setOnPreparedListener(mPlayMusicService);
        mMediaPlayer.setOnCompletionListener(mPlayMusicService);
        mMediaPlayer.prepareAsync();
    }

    @Override
    public void start() {
        mMediaPlayer.start();
    }

    @Override
    public void pause() {
        mMediaPlayer.pause();
    }

    @Override
    public void previous() {
        changeTrack(getPreviousTrack());
    }

    private Track getPreviousTrack() {
        int position = mTracks.indexOf(mCurrentTrack);
        if (position == 0) {
            return mTracks.get(mTracks.size() - 1);
        }
        return mTracks.get(position - 1);
    }

    @Override
    public void next() {
        if (getShuffleType() == ShuffleType.OFF) {
            changeTrack(getNextTrack());
        } else {
            changeTrack(getRandomTrack());
        }
    }

    private Track getRandomTrack() {
        Random random = new Random();
        return mTracks.get(random.nextInt(mTracks.size()));
    }

    private Track getNextTrack() {
        int position = mTracks.indexOf(mCurrentTrack);
        if (position == mTracks.size() - 1) {
            return mTracks.get(0);
        }
        return mTracks.get(position + 1);
    }

    @Override
    public long getDuration() {
        return mMediaPlayer.getDuration();
    }

    @Override
    public long getCurrentDuration() {
        return mMediaPlayer.getCurrentPosition();
    }

    @Override
    public void shuffle() {
        setShuffleType(ShuffleType.ON);
    }

    @Override
    public void unShuffle() {
        setShuffleType(ShuffleType.OFF);
    }

    @Override
    public boolean isPlaying() {
        return mMediaPlayer.isPlaying();
    }

    @Override
    public void stop() {
        mMediaPlayer.stop();
    }

    @Override
    public void seek(int position) {
        mMediaPlayer.seekTo(position);
    }

    @Override
    public void addTrack(Track track) {
        mTracks.add(track);
    }

    @Override
    public void addTracks(List<Track> tracks) {
        mTracks.addAll(tracks);
    }

    @Override
    public void changeTrack(Track track) {
        mCurrentTrack = track;
        create(track);
    }

    @Override
    public void release() {
        mMediaPlayer.release();
    }

    public List<Track> getTracks() {
        return mTracks;
    }

    public boolean isLastTrack() {
        int position = mTracks.indexOf(mCurrentTrack);
        if (position == mTracks.size() - 1) {
            return true;
        }
        return false;
    }
}