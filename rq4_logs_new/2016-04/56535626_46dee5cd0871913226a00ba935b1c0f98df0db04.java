package com.numan1617.tfltubedepartures;

import android.app.Application;

public class App extends Application {
  @Override public void onCreate() {
    super.onCreate();
    AppModule.setApplication(this);
  }
}