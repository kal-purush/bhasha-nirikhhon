 stopMediaPlayer();


        VoIPUtil.setAudioMode(LittlecApp.mContext, AudioManager.MODE_RINGTONE);
//        VoIPUtil.setSpeakerOn(LittlecApp.mContext, false);
//        AudioManager audioManager = (AudioManager) LittlecApp.mContext.getSystemService(Context.AUDIO_SERVICE);
//        audioManager.setMode(AudioManager.MODE_RINGTONE);
//
//        audioManager.requestAudioFocus(null, AudioManager.STREAM_RING, AudioManager.AUDIOFOCUS_GAIN_TRANSIENT);

        Uri ringUri = RingtoneManager.getActualDefaultRingtoneUri(mContext, RingtoneManager.TYPE_RINGTONE);
        mMediaPlayer = MediaPlayer.create(mContext, ringUri);
        mMediaPlayer.setLooping(true);
        try {
            mMediaPlayer.prepare();
        } catch (IllegalStateException | IOException e) {
            e.printStackTrace();
        }
        mMediaPlayer.start();