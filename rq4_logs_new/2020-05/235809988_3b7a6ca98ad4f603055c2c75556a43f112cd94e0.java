package com.group4sweng.scranplan.Adverts;

import android.view.View;

import androidx.annotation.NonNull;

import com.google.android.gms.ads.formats.MediaView;
import com.google.android.gms.ads.formats.UnifiedNativeAdView;
import com.group4sweng.scranplan.R;
import com.group4sweng.scranplan.SearchFunctions.HomeRecyclerAdapter;

public class AdViewHolder extends HomeRecyclerAdapter.ViewHolder {
    private UnifiedNativeAdView adView;

    public UnifiedNativeAdView getAdView() { return adView; }

    public AdViewHolder(@NonNull View itemView) {
        super(itemView);
        adView = itemView.findViewById(R.id.ad_view);

        // The MediaView will display a video asset if one is present in the ad, and the
        // first image asset otherwise.
        adView.setMediaView((MediaView) adView.findViewById(R.id.ad_media));

        // Register the view used for each individual asset.
        adView.setHeadlineView(adView.findViewById(R.id.ad_headline));
        adView.setBodyView(adView.findViewById(R.id.ad_body));
        adView.setCallToActionView(adView.findViewById(R.id.ad_call_to_action));
        adView.setIconView(adView.findViewById(R.id.ad_icon));
        adView.setPriceView(adView.findViewById(R.id.ad_price));
        adView.setStarRatingView(adView.findViewById(R.id.ad_stars));
        adView.setStoreView(adView.findViewById(R.id.ad_store));
        adView.setAdvertiserView(adView.findViewById(R.id.ad_advertiser));
    }
}