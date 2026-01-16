package com.softwareoverflow.colorfall;

import android.content.Context;
import android.util.Log;

import com.google.android.gms.ads.AdListener;
import com.google.android.gms.ads.AdRequest;
import com.google.android.gms.ads.AdSize;
import com.google.android.gms.ads.AdView;
import com.google.android.gms.ads.InterstitialAd;

public class AdvertHandler {

    private static AdView gameBannerAd;

    public void setupGameBanner(Context context){
        gameBannerAd = new AdView(context);
        gameBannerAd.setAdSize(AdSize.SMART_BANNER);
        gameBannerAd.setAdUnitId(context.getString(R.string.game_banner_ad_id));
        gameBannerAd.loadAd(new AdRequest.Builder().build());
        gameBannerAd.setAdListener( new AdListener(){
            @Override
            public void onAdLoaded() {
                Log.d("adverts", "banner ad loaded (then paused)! " + gameBannerAd.hashCode());
                gameBannerAd.pause();
            }
        });
    }

    public AdView getGameBannerAd(){
        return gameBannerAd;
    }

    public InterstitialAd createInterstitialAd(Context context){
        InterstitialAd interstitialAd = new InterstitialAd(context);
        interstitialAd.setAdUnitId(context.getString(R.string.end_game_interstitial_ad));
        interstitialAd.loadAd(new AdRequest.Builder().build());
        return interstitialAd;
    }

}