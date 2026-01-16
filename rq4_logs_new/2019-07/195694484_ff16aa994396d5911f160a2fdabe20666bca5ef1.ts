import { isIOS, screen } from "tns-core-modules/platform";
import { QuestionViewModel } from "~/question/question-view-model";
import { PersistenceService } from "~/services/persistence.service";
import {
    AD_SIZE,
    createBanner,
    createInterstitial,
    hideBanner,
    preloadInterstitial,
    showInterstitial
} from "../admob/ads.js";
import { HttpService } from "../services/http.service";
import * as constantsModule from "../shared/constants";

export class AdService {

    get showAd(): boolean {
        return this._showAd;
    }

    set showAd(showAd: boolean) {
        this._showAd = showAd;
    }

    static _testing = true;

    static getInstance(): AdService {
        return AdService._instance;
    }

    private static _instance: AdService = new AdService();
    private _showAd: boolean;

    constructor() {
        this._showAd = true;
        if (!PersistenceService.getInstance().isPremium()) {
            HttpService.getInstance().showAds().then((show) => {
                this._showAd = show === "true";
            });
        } else {
            this._showAd = false;
        }
    }

    showInterstitial() {
        if (this._showAd) {
            this.doShowInterstitial();
        }
    }

    showSmartBanner(): Promise<void> {
        if (this._showAd) {
            return this.doCreateSmartBanner();
        }
    }

    hideAd() {
        if (this._showAd) {
            hideBanner().then(
                () => console.log("Banner hidden"),
                (error) => console.error("Error hiding banner: " + error)
            );
        }
    }

    getAdHeight(): number {
        let height = 0;
        if (this._showAd) {
            const screenHeight: number = screen.mainScreen.heightDIPs;
            if (screenHeight > 400 && screenHeight < 721) {
                height = 50;
            } else if (screenHeight > 720) {
                height = 90;
            }
        }

        return height;
    }

    doCreateSmartBanner(): Promise<void> {
        return this.createBanner(AD_SIZE.SMART_BANNER);
    }

    /*doCreateSkyscraperBanner(): void {
        this.createBanner(AD_SIZE.SKYSCRAPER);
    }

    doCreateLargeBanner(): void {
        this.createBanner(AD_SIZE.LARGE_BANNER);
    }

    doCreateRegularBanner(): void {
        this.createBanner(AD_SIZE.BANNER);
    }

    doCreateRectangularBanner(): void {
        this.createBanner(AD_SIZE.MEDIUM_RECTANGLE);
    }

    doCreateLeaderboardBanner(): void {
        this.createBanner(AD_SIZE.LEADERBOARD);
    }*/

    doShowInterstitial(): void {
        if (this._showAd) {
            showInterstitial().then(
                () => console.log("Shown interstetial..."),
                (error) => console.log("Error showing interstitial", error)
            );
        }
    }

    doPreloadInterstitial(resolve, reject): void {
        if (this._showAd) {
            preloadInterstitial({
                testing: AdService._testing,
                iosInterstitialId: constantsModule.INTERSTITIAL_AD_ID,
                androidInterstitialId: constantsModule.INTERSTITIAL_AD_ID,
                onAdClosed: () => {
                    this.doPreloadInterstitial(resolve, reject);
                }
            }).then(
                () => {
                    console.log("Interstitial preloaded");
                    resolve();
                },
                (error) => {
                    console.log("Error preloading interstitial: " + error);
                    reject(error);
                }
            );
        }

    }

    doCreateInterstitial(): void {
        if (this._showAd) {
            createInterstitial({
                testing: AdService._testing,
                iosInterstitialId: constantsModule.INTERSTITIAL_AD_ID,
                androidInterstitialId: constantsModule.INTERSTITIAL_AD_ID,
                onAdClosed: () => {
                    console.log("doCreate Closed...");
                }
            }).then(
                () => console.log("Interstitial created"),
                (error) => console.error("Error creating interstitial: " + error)
            );
        }
    }

    delayedPreloadInterstitial(): void {
        setTimeout(() => {
            if (!PersistenceService.getInstance().isPremium()) {
                AdService.getInstance().doPreloadInterstitial(() => {
                        QuestionViewModel._errorLoading = false;
                    },
                    () => {
                        QuestionViewModel._errorLoading = true;
                    });
            }
        }, 2000);
    }

    private createBanner(size: AD_SIZE): Promise<void> {
        return createBanner({
            testing: AdService._testing,
            // if this 'view' property is not set, the banner is overlayed on the current top most view
            // view: ..,
            size,
            iosBannerId: constantsModule.BANNER_AD_ID,
            androidBannerId: constantsModule.BANNER_AD_ID, // our registered banner id
            // Android automatically adds the connected device as test device with testing:true, iOS does not
            // iosTestDeviceIds: ["yourTestDeviceUDIDs", "canBeAddedHere"],
            margins: {
                // if both are set, top wins
                // top: 10
                bottom: isIOS ? 50 : 0
            },
            keywords: ["games", "education"]
        });
    }
}