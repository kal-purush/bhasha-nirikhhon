import {
    AD_SIZE,
    adLoaded,
    createBanner,
    createInterstitial,
    hideBanner,
    preloadInterstitial,
    preloadVideoAd,
    showInterstitial,
    showVideoAd
} from "../admob/ads-android.js";

export class CoolAds {

    static getInstance(): CoolAds {
        return CoolAds._instance;
    }

    private static _instance: CoolAds = new CoolAds();

    createBanner(arg): Promise<any> {
       return createBanner(arg);
    }

    createInterstitial(arg): Promise<any> {
        return createInterstitial(arg);
    }

    hideBanner(): Promise<any> {
        return hideBanner();
    }

    preloadInterstitial(arg): Promise<any> {
        return preloadInterstitial(arg);
    }

    showInterstitial(): Promise<any> {
        return showInterstitial();
    }

    preloadVideoAd(arg, rewardCB, reload, afterAdLoaded): Promise<any> {
        return preloadVideoAd(arg, rewardCB, reload, afterAdLoaded);
    }

    showVideoAd(): Promise<any> {
        return showVideoAd();
    }

    adLoaded(): boolean {
        return adLoaded();
    }
}