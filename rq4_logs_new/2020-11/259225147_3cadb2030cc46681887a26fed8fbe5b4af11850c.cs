using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BGM_Fade_2 : MonoBehaviour
{
    private BGM_Manager_2 bgm_Manager;
    private BGM_InformationCollection_2 bgm_InformationCollection;

    [Header("フェードインのスピード")]
    [Range(0, 1)]
    [SerializeField] float fadeInSpead = 0.01f;
    [Header("フェードアウトのスピード")]
    [Range(0, 1)]
    [SerializeField] float fadeOutSpead = 0.02f;

    /// <summary>
    /// フェード処理をしているとき true、していないとき false
    /// </summary>
    public static bool fadeModeOn { get; set; }
    private void Start()
    {
        bgm_Manager = GetComponent<BGM_Manager_2>();
        bgm_InformationCollection = GetComponent<BGM_InformationCollection_2>();
    }
    public void Update()
    {
        for (int i = 0; i < bgm_InformationCollection.bgmDataList.Count; i++)
        {
            if(bgm_InformationCollection.bgmDataList[i].bgmFadeInNow)
            {
                BGM_Manager_2.BgmFadeInModeOn(bgm_InformationCollection.bgmDataList[i].bgmName, bgm_InformationCollection.bgmDataList[i].bgmType, fadeInSpead);
            }

            if (bgm_InformationCollection.bgmDataList[i].bgmFadeOutNow)
            {
                BGM_Manager_2.BgmFadeOutModeOn(bgm_InformationCollection.bgmDataList[i].bgmName, bgm_InformationCollection.bgmDataList[i].bgmType, fadeOutSpead);
            }
        }
    }
}