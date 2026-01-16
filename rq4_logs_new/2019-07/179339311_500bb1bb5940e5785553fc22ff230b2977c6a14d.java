package com.example.enigmator.utils;

import android.content.Context;
import android.net.Uri;

import com.example.enigmator.R;
import com.google.android.exoplayer2.DefaultLoadControl;
import com.google.android.exoplayer2.DefaultRenderersFactory;
import com.google.android.exoplayer2.ExoPlayer;
import com.google.android.exoplayer2.ExoPlayerFactory;
import com.google.android.exoplayer2.extractor.DefaultExtractorsFactory;
import com.google.android.exoplayer2.source.ExtractorMediaSource;
import com.google.android.exoplayer2.source.MediaSource;
import com.google.android.exoplayer2.trackselection.DefaultTrackSelector;
import com.google.android.exoplayer2.upstream.DefaultDataSourceFactory;
import com.google.android.exoplayer2.util.Util;

import java.io.File;

public class MediaPlayer {
    private ExoPlayer player;
    private Context context;

    private void initializePlayer() {
        player = ExoPlayerFactory.newSimpleInstance(
                new DefaultRenderersFactory(context),
                new DefaultTrackSelector(),
                new DefaultLoadControl()
        );
    }

    public void play(String fileName) {
        String userAgent = Util.getUserAgent(context,
                context.getString(R.string.app_name));

        /* MediaSource mediaSource =*/
        MediaSource mediaSource = new ExtractorMediaSource.Factory(
                new DefaultDataSourceFactory(context, userAgent))
                .setExtractorsFactory(new DefaultExtractorsFactory())
                .createMediaSource(Uri.fromFile(new File(fileName)));

        player.prepare(mediaSource);
        player.getPlayWhenReady();
    }

    public ExoPlayer getPlayerImpl(Context context) {
        this.context = context;
        initializePlayer();
        return player;
    }
}