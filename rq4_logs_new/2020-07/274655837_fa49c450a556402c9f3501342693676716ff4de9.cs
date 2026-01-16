using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Advertisements;
using UnityEngine.UI;

public class Ads : MonoBehaviour
{
    private static Ads instance;

    private const string ID = "3739541", TYPE = "video";
    private const bool TEST_MODE = true;

    public static Ads getInstance()
    {
        if (instance == null)
        {
            instance = new Ads();
            Advertisement.Initialize(ID, TEST_MODE);
        }
        return instance;
    }

    public IEnumerator StartAd(int money, Text text)
    {
        while (true)
        {
            if (Advertisement.IsReady(TYPE))
            {
                Advertisement.Show(TYPE);
                // if x3
                // money *= 2;
                text.text = (int.Parse(text.text) + money).ToString();
                PlayerData.getInstance().money += money;
                PlayerData.getInstance().Save();
                yield break;
            }
            yield return new WaitForSeconds(1f);
        }
    }
}
  