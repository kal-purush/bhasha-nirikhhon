using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ThemeMangaer : Singleton<ThemeMangaer> 
{
    [SerializeField] private GameObject homeCanvas;
    [SerializeField] private GameObject playCanvas;
    [SerializeField] private GameObject pauseCanvas;

    [SerializeField] private Button w1Btn;
    [SerializeField] private Button w2Btn;
    [SerializeField] private Button w3Btn;
    [SerializeField] private Button w4Btn;

    private void Start()
    {
        
    }

    public void OnClickW1Btn()
    {
    }
}