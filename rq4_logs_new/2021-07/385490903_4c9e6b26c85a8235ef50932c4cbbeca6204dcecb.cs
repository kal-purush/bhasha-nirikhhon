using UnityEngine;
using UnityEngine.Advertisements;
using UnityEngine.SceneManagement;
using UnityEngine.UI;
using UnityEngine.UIElements;


public class AdManager : MonoBehaviour, IUnityAdsListener
{
    ShowResult showResult;
    string gameId = "3713997";
    string myPlacementId = "rewardedVideo";
    bool testMode = false;
    public GameObject messager;
    public GameObject CounterUI;
    public AudioSource back;
    public AudioSource sou;
    public void Resume()
    {
        SceneManager.LoadScene("game");
    }
    public void Quit()
    {
        SceneManager.LoadScene("MAinmenu");
    }
    void Start()
    {
        Advertisement.AddListener(this);
        Advertisement.Initialize(gameId, testMode);
    }
    public void Cancel()
    {
        sou.Play();
        back.volume = 1;
        messager.SetActive(false);
        CounterUI.SetActive(false);
    }
    public void ShowRewardedVideo()
    {
        // Check if UnityAds ready before calling Show method:
        if (Advertisement.IsReady(myPlacementId))
        {
            Advertisement.Show(myPlacementId);
            showResult = ShowResult.Finished;
            scorescript.c = 1;
        }
        else
        {
            if (PlayerPrefs.GetInt("MutedSound", 0) == 0)
            {
                sou.volume = 1;
            }
            else
            {
                sou.volume = 0;
            }
            back.volume = 0;
            messager.SetActive(true);
            showResult = ShowResult.Failed;
        }
    }

    // Implement IUnityAdsListener interface methods:
    public void OnUnityAdsDidFinish(string placementId, ShowResult showResult)
    {
        // Define conditional logic for each ad completion status:
        if (showResult == ShowResult.Finished)
        {
            // Reward the user for watching the ad to completion.
            Resume();
        }
        else if (showResult == ShowResult.Skipped)
        {
            messager.SetActive(true);
            // Do not reward the user for skipping the ad.
        }
        else if (showResult == ShowResult.Failed)
        {
            messager.SetActive(true);

        }
    }

    public void OnUnityAdsReady(string placementId)
    {
        // If the ready Placement is rewarded, show the ad:
        if (placementId == myPlacementId)
        {
            // Optional actions to take when the placement becomes ready(For example, enable the rewarded ads button)
        }
    }

    public void OnUnityAdsDidError(string message)
    {
        // Log the error.
    }

    public void OnUnityAdsDidStart(string placementId)
    {
        // Optional actions to take when the end-users triggers an ad.
    }

    // When the object that subscribes to ad events is destroyed, remove the listener:
    public void OnDestroy()
    {
        Advertisement.RemoveListener(this);
    }
    public void video()
    {
        if (scorescript.c == 0)
        {
            ShowRewardedVideo();
            OnUnityAdsDidFinish(myPlacementId, showResult);
        }
    }
}