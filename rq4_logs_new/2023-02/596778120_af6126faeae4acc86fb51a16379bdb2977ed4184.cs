using SonicBloom.Koreo;
using SonicBloom.Koreo.Players;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;


public class GameManager : MonoBehaviour
{
    public static GameManager Instance;
    
    //Aduio
    private AudioManager _KoreoPlayer;
    private bool _isKoreoPlayerNull = true;
    private Koreography _stage;
    //Global Volume
    private GlobalVolumeManager _globalVolume;
    private bool _isGlobalVolumeNull = true;

    private void Awake()
    {
        ManagerInstance();
    }
    void Start()
    {
        StartCoroutine(CheckNullKoreoPlayer());

        StartCoroutine(StartingGame());

        StartCoroutine(AccessGlobalVolumeInstance());
    }
    private void StartGame()
    {
        Debug.Log("StartGame");
        //AudioManger
        _stage = _KoreoPlayer.KoreoStage1;
        _KoreoPlayer.LoadKoreoTrack(_stage, 0, false);
        _KoreoPlayer.PlayKoreoTrack(true);
        //GlobalVolume setup.
        _globalVolume.SetDepthOfField(0.0f, 93.3f);
    }
    IEnumerator AccessGlobalVolumeInstance()
    {
        for (int i = 0; i < 10; i++)
        {
            if (GlobalVolumeManager.Instance == null)
                yield return new WaitForEndOfFrame();
            else
                continue;
            if (i >= 10)
            {
                Debug.Log("AudioManger Instance was not set by script in 10 frames ", transform);
                yield return null;
            }
        }
        Debug.Log("Loaded Volume Instance to GameManger");
        _isGlobalVolumeNull = false;
        _globalVolume = GlobalVolumeManager.Instance;
        yield return null;
    }
    IEnumerator StartingGame()
    {
        while (_isKoreoPlayerNull || _isGlobalVolumeNull)
        {
            yield return new WaitForEndOfFrame();
        }
        StartGame();
        _isKoreoPlayerNull = false;
        yield return null;
    }
    IEnumerator CheckNullKoreoPlayer()
    {
        for(int i = 0; i < 10; i++) 
        {
            if (AudioManager.Instance == null)
                yield return new WaitForEndOfFrame();
            else
                continue;
            if(i >= 10)
            {
                Debug.Log("AudioManger Instance was not set by script in 10 frames ", transform);
                yield return null;
            }
        }
        Debug.Log("Loaded Audiomanger Instance to GameManger");
        _isKoreoPlayerNull = false;
        _KoreoPlayer = AudioManager.Instance;
        yield return null;
    }
    private void ManagerInstance()
    {
        if (Instance == null)
            Instance = this;
        else
            Destroy(gameObject);
    }
    // Update is called once per frame
    void Update()
    {
        
    }
}