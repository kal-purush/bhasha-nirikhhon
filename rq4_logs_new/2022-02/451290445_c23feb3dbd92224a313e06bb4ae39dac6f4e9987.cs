using System;
using System.Linq;
using System.Text;
using DG.Tweening;
using UnityEngine;

public class DontDestroyOnLoadCanvas : MonoBehaviour
{

    public static DontDestroyOnLoadCanvas instance;

    private void Awake()
    {
        if (instance == null)
        {
            instance = this;
            Screen.sleepTimeout = SleepTimeout.NeverSleep;
            // QualitySettings.vSyncCount = 0;
            Application.targetFrameRate = LocalStorageUtils.GetFps();
            Application.runInBackground = true;
            DontDestroyOnLoad(this);
        }
        else if (instance != this)
            Destroy(gameObject);
        
    }

}