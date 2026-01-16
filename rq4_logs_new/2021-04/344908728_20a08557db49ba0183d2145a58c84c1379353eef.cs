using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Monetization;

public class AdsControllerCoins : MonoBehaviour
{

    public int numberOfCoins;

    private string appId = "4089943";
    private string coinAds = "Coins";
    private int coins = 0;
    // Start is called before the first frame update
    void Start()
    {
        Monetization.Initialize(appId, true);
    }
    public void showAds()
    {
        if (Monetization.IsReady(coinAds))
        {
            ShowAdPlacementContent ad = null;
            ad = Monetization.GetPlacementContent(coinAds) as ShowAdPlacementContent;
            if (ad != null)
            {
                ad.Show();
            }
            setCoinAds();
        }
    }
    private void setCoinAds()
    {
        coins += numberOfCoins;
    }
}