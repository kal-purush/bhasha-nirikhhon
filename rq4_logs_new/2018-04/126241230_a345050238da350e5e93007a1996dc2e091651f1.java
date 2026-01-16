/*
 * Copyright 2014 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gvideo.sgutierc.cl.videorecorder;

import android.app.Activity;
import android.util.Log;

import java.io.File;

import gvideo.sgutierc.cl.location.LocationRecorder;
import gvideo.sgutierc.cl.util.Miscelaneous;
import gvideo.sgutierc.cl.view.AutoFitTextureView;

/**
 *
 */
public class GeoVideoEngine extends VideoEngine {
    private LocationEngine locationEngine;
    private LocationRecorder recorder;

    /**
     * @param activity
     * @param mTextureView
     * @param pathProvider
     * @param locationEngine
     */
    public GeoVideoEngine(Activity activity, AutoFitTextureView mTextureView, PathProvider pathProvider, LocationEngine locationEngine) {
        super(activity, mTextureView, pathProvider);
        this.locationEngine = locationEngine;
        //and now start listening locations
        recorder = new LocationRecorder(activity);
    }

    @Override
    public void startRecording() {
        if (null == mCameraDevice || !mTextureView.isAvailable() || null == mPreviewSize) {
            return;
        }
        try {
            //inicia mecanismo de geolocalización
            recorder.init();
            recorder.start(locationEngine);
            super.startRecording();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stopRecording() {
        //Stop listening GPS data
        locationEngine.removeHandler(recorder);
        recorder.stop();
        byte[] locations = null;
        try {
            locations = recorder.getBytes();
            Log.i(this.getClass().getName(), Miscelaneous.print(locations));

        } catch (Exception e) {
            e.printStackTrace();
        }
        String videoPath = getVideoPath();
        super.stopRecording();
        Miscelaneous.writeMetadata(getActivity(), new File(videoPath), "LOCALIZACION", locations);
    }

    @Override
    public void pauseRecordingMajor() {
        recorder.pause();
        super.pauseRecordingMajor();
    }

    @Override
    public void resumeRecordingMajor() {
        recorder.resume();
        super.resumeRecordingMajor();
    }
}