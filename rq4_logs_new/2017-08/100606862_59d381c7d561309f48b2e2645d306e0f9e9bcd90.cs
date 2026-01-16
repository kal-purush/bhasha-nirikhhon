using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PanelRanking : MonoBehaviour {

    public RankingGo[] rankingGos;

    private void OnEnable()
    {
        PhpQuery.GetRanking(OnGeted);
    }

    private void OnGeted(string obj)
    {
        List<Shop> list = JsonParser<List<Shop>>.GetObject(obj);
        for (int i = 0; i < 5; i++)
        {
            rankingGos[i].name.text = list[i].name;
        }
    }

    [System.Serializable]
    public class RankingGo
    {
        public Text name;
        public Image imageShop;
    }
}