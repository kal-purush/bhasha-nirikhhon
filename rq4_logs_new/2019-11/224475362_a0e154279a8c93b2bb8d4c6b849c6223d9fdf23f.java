package com.example.android.common.media;

import android.media.AudioFormat;
import android.media.AudioRecord;
import android.util.Log;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class AudioRecordHelper {
    private static final String TAG = "AudioRecordHelper";
    private static final int RECORDER_SAMPLERATE = 8000;
    private static final int RECORDER_CHANNELS = AudioFormat.CHANNEL_IN_MONO;
    private static final int RECORDER_AUDIO_ENCODING = AudioFormat.ENCODING_PCM_16BIT;

    private static int sBufferElements2Rec = 1024; // want to play 2048 (2K) since 2 bytes we use only 1024
    private static int sBytesPerElement = 2; // 2 bytes in 16bit format
    private static boolean sIsRecording = false;
    private static AudioRecord mRecorder;

    public static void startRecording(int audioSource) {
        if (mRecorder != null && sIsRecording) {
            stopRecording();
        }
        int bufferSize = AudioRecord.getMinBufferSize(RECORDER_SAMPLERATE,
                RECORDER_CHANNELS, RECORDER_AUDIO_ENCODING);
        if (bufferSize <= sBufferElements2Rec * sBytesPerElement) {
            throw new IllegalStateException("Buffer smaller than MinBufferSize");
        }

        mRecorder = new AudioRecord(audioSource,
                RECORDER_SAMPLERATE, RECORDER_CHANNELS,
                RECORDER_AUDIO_ENCODING, bufferSize);
        mRecorder.startRecording();
        sIsRecording = true;
        new Thread(new Runnable() {
            public void run() {
                writeAudioDataToFile(mRecorder);
            }
        }, "AudioRecorder Thread").start();
        Log.d(TAG, "recording started");
    }

    //convert short to byte
    private static byte[] short2byte(short[] sData) {
        int shortArrsize = sData.length;
        byte[] bytes = new byte[shortArrsize * 2];
        for (int i = 0; i < shortArrsize; i++) {
            bytes[i * 2] = (byte) (sData[i] & 0x00FF);
            bytes[(i * 2) + 1] = (byte) (sData[i] >> 8);
            sData[i] = 0;
        }
        return bytes;

    }

    private static boolean writeAudioDataToFile(AudioRecord recorder) {
        // Write the output audio in byte

        String filePath = "/sdcard/voice8K16bitmono.pcm";
        short sData[] = new short[sBufferElements2Rec];

        FileOutputStream os = null;
        try {
            os = new FileOutputStream(filePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }

        while (sIsRecording) {
            // gets the voice output from microphone to byte format

            recorder.read(sData, 0, sBufferElements2Rec);
            System.out.println("Short writing to file" + sData.toString());
            try {
                // // writes the data to file from buffer
                // // stores the voice buffer
                byte bData[] = short2byte(sData);
                os.write(bData, 0, sBufferElements2Rec * sBytesPerElement);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    public static void stopRecording() {
        Log.d(TAG, "recording stopped");
        if (mRecorder != null) {
            sIsRecording = false;
            mRecorder.stop();
            mRecorder.release();
            mRecorder = null;
        }
    }
}