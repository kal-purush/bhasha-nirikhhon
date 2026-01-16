using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CurrentUser : MonoBehaviour
{
    [SerializeField] private MainCanvas mainCanvas;
    [SerializeField] private EnterCanvas enterCanvas;
    private const string USER_ID = "USER_ID";
    private int user_ID;

    private void Awake()
    {
        if (PlayerPrefs.HasKey(USER_ID))
        {
            user_ID = PlayerPrefs.GetInt(USER_ID);
            enterCanvas.HideCanvas();
            mainCanvas.ShowCanvas();
        }
    }
}