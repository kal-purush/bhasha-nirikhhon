using UnityEngine;
using UnityEngine.Advertisements;

public class AdsManager : MonoBehaviour, IUnityAdsListener
{
    public static AdsManager AdController { get; private set; }
    private string appleStoreID = "3576342";
    private string googleStoreID = "3576343";

    private string interstitialAd = "video";
    private string rewardedVideoAd = "rewardedVideo";

    [SerializeField]
    private bool isTestAd;
    [SerializeField]
    private bool isGoogleStore;
    [SerializeField]
    private float adTimer = 180.0f;
    [SerializeField]
    private Music_Player mPlayer;
    public bool adStarted;

    private void Awake()
    {
        if (AdController == null)
        {
            AdController = this;
            DontDestroyOnLoad(gameObject);
        }
        else
            Destroy(this);
    }

    private void Start()
    {
        Advertisement.AddListener(this);
        InitializeAd();
    }

    private void Update()
    {
        if (adTimer > 0.0f)
        {
            adTimer -= Time.unscaledDeltaTime;
        }
        else
            playAd();
    }

    private void playAd()
    {
        PlayInterstitialAd();
        adTimer = 25.0f;
    }
    private void InitializeAd()
    {
        if(isGoogleStore)
        {
            Advertisement.Initialize(googleStoreID, isTestAd);
            return;
        }
        Advertisement.Initialize(appleStoreID, isTestAd);
    }

    public void PlayInterstitialAd()
    {
        if(!Advertisement.IsReady(interstitialAd))
            return;
        Advertisement.Show(interstitialAd);
    }

    public void PlayRewardedVideoAd()
    {
        if (!Advertisement.IsReady(rewardedVideoAd))
            return;
        Advertisement.Show(rewardedVideoAd);
    }

    public void OnUnityAdsReady(string placementId){}

    public void OnUnityAdsDidError(string message){}

    public void OnUnityAdsDidStart(string placementId)
    {
        adStarted = true;
        mPlayer = (Music_Player)FindObjectOfType(typeof(Music_Player));
        if (mPlayer)
            mPlayer.adAudioMute();
        else
            Debug.Log("Couldn't find Music_player in scene");
        Time.timeScale = 0;
    }

    public void OnUnityAdsDidFinish(string placementId, ShowResult showResult)
    {
        switch (showResult)
        {
            case ShowResult.Finished:
                break;
            case ShowResult.Skipped:
                break;
            case ShowResult.Failed:
                break;
        }
        mPlayer.adAudioUnmute();
        Time.timeScale = 1;
        adStarted = false;
    }
}