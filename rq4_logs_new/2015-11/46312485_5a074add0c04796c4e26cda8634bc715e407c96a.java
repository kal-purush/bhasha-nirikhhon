package com.locationlabs.porchlight;

import android.app.Application;
import android.content.Context;

import com.locationlabs.porchlight.util.DataStore;
import com.locationlabs.porchlight.util.pubnub.PubNubUtil;

/**
 * Application class of the app
 */
public class PorchlightApplication extends Application {

   private static final boolean MOCKED_APP = false;

   private static PorchlightApplication mInstance;

   @Override
   public void onCreate() {
      super.onCreate();
      mInstance = this;

      // Initializes libs
      PubNubUtil.initialize();
   }

   public static Context getContext() {
      return mInstance;
   }

   public static boolean isMocked() {
      return MOCKED_APP;
   }
}