package com.progettofondamenti.equalizeraudioplayer;

import android.test.ActivityInstrumentationTestCase2;
import android.widget.TextView;

/**
 * Junit Test Class
 * Tests the launching of the mediaplayer
 * @author team
 */
public class TestOnLaunchingMediaPlayer extends ActivityInstrumentationTestCase2<MainActivity> {

    private MainActivity mainActivity;
    private TextView songText;
    private TextView elapsedTime;
    private TextView remainingTime;
    private static int PLAY_TIME = 30000;

    public TestOnLaunchingMediaPlayer() {
        super(MainActivity.class);
    }

    /*
     * non-javadoc
     * sets up the activity for usage
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mainActivity = getActivity();
        songText = (TextView) mainActivity.findViewById(R.id.songTitle);
        elapsedTime= (TextView)mainActivity.findViewById(R.id.elapsedTime);
        remainingTime = (TextView)mainActivity.findViewById(R.id.remainingTime);
        getActivity();
    }

    /**
     * Launch music player and sleep for 30 seconds to capture
     * the music player power usage base line
     *
     * @throws Exception
     */
    public void testLaunchMusicPlayer() throws Exception {

        try {
            Thread.sleep(PLAY_TIME);
        } catch (Exception e) {
            assertTrue("MusicPlayer Do Nothing", false);
        }
        assertTrue("MusicPlayer Do Nothing", true);
    }

}