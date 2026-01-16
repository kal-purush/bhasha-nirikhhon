package ru.ifmo.android_2015.homework5;

import android.app.IntentService;
import android.content.Intent;
import android.util.Log;

import ru.ifmo.android_2015.homework5.InitSplashActivity.DownloadState;

import java.io.IOException;

/**
 * Created by lalala on 27.12.15.
 */
public class DownloadService extends IntentService {

    public DownloadService() {
        super("DownloadService");
    }
    /**
     * Creates an IntentService.  Invoked by your subclass's constructor.
     *
     * @param name Used to name the worker thread, important only for debugging.
     */
    public DownloadService(String name) {
        super(name);
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        Log.d("Service", "Service");
        try {
            InitSplashActivity.downloadFile(this, new ProgressCallback() {
                @Override
                public void onProgressChanged(int progress) {
                    publishProgress(((progress < 100) ? DownloadState.DOWNLOADING : DownloadState.DONE), progress);
                }
            });
            Log.d("Service", "Done");
        } catch (IOException e) {
            publishProgress(DownloadState.ERROR, 0);
            e.printStackTrace();
        }
    }

    private void publishProgress(DownloadState state, int progress) {
        Intent intent = new Intent(InitSplashActivity.FILTER);
        intent.putExtra(InitSplashActivity.PROGRESS, progress);
        intent.putExtra(InitSplashActivity.STATE, state);
        sendBroadcast(intent);
    }
}