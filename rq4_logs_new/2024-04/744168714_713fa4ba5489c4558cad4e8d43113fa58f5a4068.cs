using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using FMODUnity;
using FMOD.Studio;

public class MainMenu : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        SoundManager.Instance.menu = SoundManager.Instance.CreateInstance(FMODEvents.Instance.mainMenu);
        SoundManager.Instance.menu.start();
        SoundManager.Instance.menu.release();
    }

    // Update is called once per frame
    public void nextScene()
    {
        SoundManager.Instance.menu.stop(FMOD.Studio.STOP_MODE.ALLOWFADEOUT);
        SoundManager.Instance.wave0.start();
        SoundManager.Instance.wave0.release();

    }
}