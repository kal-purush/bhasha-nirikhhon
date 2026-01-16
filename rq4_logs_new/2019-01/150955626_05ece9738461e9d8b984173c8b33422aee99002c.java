package wangfeixixi.com.base;

import android.content.Context;
import android.content.res.AssetFileDescriptor;
import android.media.AudioManager;
import android.media.MediaPlayer;
import android.os.Vibrator;

import wangfeixixi.com.base.mvvm.base.BaseApplication;

public class MediaUtils {

    private MediaPlayer mediaPlayer;
    private static Vibrator vibrator;

    private MediaUtils() {
        AudioManager audioService = (AudioManager) BaseApplication.getInstance().getSystemService(Context.AUDIO_SERVICE);
        if (audioService.getRingerMode() != AudioManager.RINGER_MODE_NORMAL) {
            return;
        }
        mediaPlayer = new MediaPlayer();
        mediaPlayer.setAudioStreamType(AudioManager.STREAM_MUSIC);
        final MediaPlayer finalMediaPlayer = mediaPlayer;
        mediaPlayer.setOnCompletionListener(new MediaPlayer.OnCompletionListener() {
            @Override
            public void onCompletion(MediaPlayer mp) {
                finalMediaPlayer.seekTo(0);
            }
        });

        AssetFileDescriptor file = UIUtils.getResources().openRawResourceFd(R.raw.beep);
        try {
            mediaPlayer.setDataSource(file.getFileDescriptor(), file.getStartOffset(), file.getLength());
            file.close();
            mediaPlayer.setVolume(1f, 1f);
            mediaPlayer.prepare();
//            if (mediaPlayer != null) {
//                mediaPlayer.start();
//            }
        } catch (Exception e) {
            e.printStackTrace();
            mediaPlayer = null;
        }

        //震动
        vibrator = (Vibrator) BaseApplication.getInstance().getSystemService(Context.VIBRATOR_SERVICE);
    }

    private static class Inner {
        private static MediaUtils instance = new MediaUtils();
    }

    public static MediaUtils getInstance() {
        return Inner.instance;
    }

    public void beep() {
        if (mediaPlayer != null) {
            mediaPlayer.start();
        }
    }

//    public static void beep() {
//        AudioManager audioService = (AudioManager) BaseApplication.getInstance().getSystemService(Context.AUDIO_SERVICE);
//        if (audioService.getRingerMode() != AudioManager.RINGER_MODE_NORMAL) {
//            return;
//        }
//        MediaPlayer mediaPlayer = new MediaPlayer();
//        mediaPlayer.setAudioStreamType(AudioManager.STREAM_MUSIC);
//        final MediaPlayer finalMediaPlayer = mediaPlayer;
//        mediaPlayer.setOnCompletionListener(new MediaPlayer.OnCompletionListener() {
//            @Override
//            public void onCompletion(MediaPlayer mp) {
//                finalMediaPlayer.seekTo(0);
//            }
//        });
//
//        AssetFileDescriptor file = UIUtils.getResources().openRawResourceFd(R.raw.beep_2);
//        try {
//            mediaPlayer.setDataSource(file.getFileDescriptor(), file.getStartOffset(), file.getLength());
//            file.close();
//            mediaPlayer.setVolume(1f, 1f);
//            mediaPlayer.prepare();
//            if (mediaPlayer != null) {
//                mediaPlayer.start();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            mediaPlayer = null;
//        }
//    }

//    public static void vibrate(boolean isSuccess) {
//        if (isSuccess) {
//            vibrator.vibrate(new long[]{200, 200, 200, 200, 200, 200}, 2);
//        } else {
//            vibrator.vibrate(new long[]{300, 300, 300, 300, 300, 300}, 2);
//        }
//    }

    public void vibrate(int time) {
        vibrator.vibrate(time);
    }

    public void vibrate(long[] longs) {
        vibrator.vibrate(longs, 1);
    }


    private int isBeep = 0;

    public void start() {
        beep();

//        switch (++isBeep) {
//            case 1:
//                beep();
////                vibrate(new long[]{500, 500});
//                break;
//            case 25:
//                isBeep = 0;
//                break;
//        }

    }
}