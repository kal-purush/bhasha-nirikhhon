
package com.reactlibrary;

import android.os.Bundle;
import android.util.Log;

import com.amazon.device.ads.AdError;
import com.amazon.device.ads.DTBAdCallback;
import com.amazon.device.ads.DTBAdRequest;
import com.amazon.device.ads.DTBAdResponse;
import com.amazon.device.ads.DTBAdSize;
import com.amazon.device.ads.DTBAdUtil;
import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.WritableMap;
import com.google.android.gms.ads.doubleclick.PublisherAdRequest;

public class RNAmazonTamModule extends ReactContextBaseJavaModule {

  private static final String AMZN_B = "amzn_b";
  private static final String AMZN_H = "amzn_h";
  private static final String DC = "dc";
  private static final String AMZNNP = "amznp";
  private static final String AMZNSLOTS = "amznslots";

  private final ReactApplicationContext reactContext;

  public RNAmazonTamModule(ReactApplicationContext reactContext) {
    super(reactContext);
    this.reactContext = reactContext;
  }

  @Override
  public String getName() {
    return "AmazonTam";
  }

  @ReactMethod
  public void requestBannerAd(
          int width,
          int height,
          String slotUUID,
          final Promise promise) {

    final DTBAdRequest loader = new DTBAdRequest();
    loader.setSizes(new DTBAdSize(width, height, slotUUID));

    loader.loadAd(new DTBAdCallback() {
      @Override
      public void onFailure(AdError adError) {
        String code = adError.getCode().toString();
        String message = "Oops banner ad load has failed: " + adError.getMessage();
        Log.e(code, message);
        promise.reject(code, message);
      }

      @Override
      public void onSuccess(DTBAdResponse dtbAdResponse) {
        final PublisherAdRequest adRequest = DTBAdUtil.INSTANCE.createPublisherAdRequestBuilder(dtbAdResponse).build();

        Bundle customTargeting = adRequest.getCustomTargeting();

        WritableMap map = Arguments.createMap();

        map.putString(AMZN_B, customTargeting.getString(AMZN_B));
        map.putString(AMZN_H, customTargeting.getString(AMZN_H));
        map.putString(DC, customTargeting.getString(DC));
        map.putString(AMZNNP, customTargeting.getString(AMZNNP));
        map.putString(AMZNSLOTS, customTargeting.getString(AMZNSLOTS));

        promise.resolve(map);
      }
    });
  }
}