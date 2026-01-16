using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BGM_BossTerritory : MonoBehaviour
{
    [SerializeField] BGM_InformationCollection.BGM_NAME stageBGMSet;
    [SerializeField] BGM_InformationCollection.BGM_NAME bossBGMSet;

    void Start()
    {
        TerritorySenses.OnTerrtoryEnter.AddListener(() => 
        { 
            BGM_Manager.BgmFadeIn(bossBGMSet);
            BGM_Manager.BgmFadeOut(stageBGMSet);
        });
        TerritorySenses.OnTerrtoryExit.AddListener(() =>
        {
            BGM_Manager.BgmFadeIn(stageBGMSet);
            BGM_Manager.BgmFadeOut(bossBGMSet);
        });
    }
}