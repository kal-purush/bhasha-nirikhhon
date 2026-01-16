package com.numan1617.tfltubedepartures;

import android.support.annotation.NonNull;
import com.numan1617.tfltubedepartures.network.TflService;
import com.numan1617.tfltubedepartures.network.retrofit.RetrofitTflService;

public class AppModule {

  private static TflService tflService;

  @NonNull
  public static TflService tflService() {
    if (tflService == null) {
      tflService = new RetrofitTflService(BuildConfig.TFL_API_APP_ID, BuildConfig.TFL_API_KEY);
    }
    return tflService;
  }
}