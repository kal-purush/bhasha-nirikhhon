package aaron.filecommand.activity;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;


import com.google.android.gms.ads.AdListener;
import com.google.android.gms.ads.AdRequest;
import com.google.android.gms.ads.InterstitialAd;

import aaron.filecommand.R;

/**
 * Created by jili on 7/3/17.
 */

public class InterAd_Activity extends AppCompatActivity {
    private String TAG = InterAd_Activity.class.getSimpleName();
    InterstitialAd mInterstitialAd;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_inter_ad);

        mInterstitialAd = new InterstitialAd(this);

        // set the ad unit ID
        mInterstitialAd.setAdUnitId( "ca-app-pub-3456168518371304/4640726822");

        AdRequest adRequest = new AdRequest.Builder()
                .build();

        // Load ads into Interstitial Ads
        mInterstitialAd.loadAd(adRequest);

        mInterstitialAd.setAdListener(new AdListener() {
            public void onAdLoaded() {
                showInterstitial();
            }

            @Override
            public void onAdClosed() {
//                startActivity(new Intent(getApplication(), MainActivity.class));
                finish();
            }
        });


    }

    private void showInterstitial() {
        if (mInterstitialAd.isLoaded()) {
            mInterstitialAd.show();
        }else{
            finish();
        }
    }

    @Override
    public void onBackPressed() {
        finish();
    }
}