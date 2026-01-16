package com.krazylabs.rnamazontam;

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


public class RNAmazonTAMInterstitial extends ReactContextBaseJavaModule {
  private final ReactApplicationContext reactContext;

  public RNAmazonTAMInterstitial(ReactApplicationContext reactContext) {
    super(reactContext);
    this.reactContext = reactContext;
  }

  @Override
  public String getName() {
    return "AmazonTAMInterstitial";
  }

  @ReactMethod
  public void requestInterstitialAd(String slotUUID, final Promise promise) {
    final DTBAdRequest adLoader = new DTBAdRequest();
    adLoader.setSizes(new DTBAdSize.DTBInterstitialAdSize(slotUUID));
    adLoader.loadAd(new DTBAdCallback() {
        @Override
        public void onFailure(AdError adError) {
          String code = adError.getCode().toString();
          String message = "Failed to get the interstitial ad from Amazon: " + adError.getMessage();
          Log.e(code, message);
          promise.reject(code, message);
        }
    
        @Override
        public void onSuccess(DTBAdResponse dtbAdResponse) {
          WritableMap customTargeting = RNAmazonTAMUtils.getCustomTargeting(dtbAdResponse);
          promise.resolve(customTargeting);
        }
    });
  }
}