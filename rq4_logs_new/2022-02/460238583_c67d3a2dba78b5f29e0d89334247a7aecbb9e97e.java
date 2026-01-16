// Copyright 2004-present Facebook. All Rights Reserved.

package com.example.android.camera2video;

public class CameraIntents {

  // intents names from the Manifest file
  public static class Intents {
    public static final String RECORD_SLOW_MOTION =
        "com.example.android.camera2video.RECORD_SLOW_MOTION";
  }

  public static class Actions {
    public static final String RECORDING = "RECORDING";
  }

  public static class Extras {
    public static final String START = "start";
    public static final String STOP = "stop";
  }
}