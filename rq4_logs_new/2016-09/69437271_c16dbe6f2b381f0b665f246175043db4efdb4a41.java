package at.juggle.sieben;

import android.content.Context;
import android.media.MediaPlayer;

import de.baumann.sieben.R;

/**
 * Created by dermotte on 28.09.2016.
 */

public class SoundPool {
    public static int sndWhistle = R.raw.whistle_blow_cc0;

    public static void playWhistle(Context context) {
        MediaPlayer.create(context, R.raw.whistle_blow_cc0).start();
    }
}