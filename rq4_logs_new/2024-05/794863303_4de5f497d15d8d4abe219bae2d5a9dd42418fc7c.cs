using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Serialization;

public class SoundManager : Singleton<SoundManager>
{
    [SerializeField]
    AudioSource _bgmAudioSource;

    [SerializeField]
    AudioSource _seAudioSource;

    [SerializeField]
    List<BGMSoundData> _bgmSoundDataList;

    [SerializeField]
    List<SeSoundData> _seSoundDataList;

    //あとで設定データに移動
    public float _masterVolume    = 1;
    public float _bgmMasterVolume = 1;
    public float _seMasterVolume  = 1;


    /// <summary>
    /// BGMを再生
    /// </summary>
    /// <param name="bgm">BGMSoundData.BGM</param>
    public void PlayBGM(BGMSoundData.BGM bgm)
    {
        BGMSoundData data = _bgmSoundDataList.Find(data => data._bgm == bgm);
        _bgmAudioSource.clip   = data._audioClip;
        _bgmAudioSource.volume = data._volume * _bgmMasterVolume * _masterVolume;
        _bgmAudioSource.Play();
    }

    /// <summary>
    /// SEを再生
    /// </summary>
    /// <param name="se">SeSoundData.Se</param>
    public void PlaySe(SeSoundData.SE se)
    {
        SeSoundData data = _seSoundDataList.Find(data => data._se == se);
        _seAudioSource.volume = data._volume * _seMasterVolume * _masterVolume;
        _seAudioSource.PlayOneShot(data._audioClip);
    }

    public override void AwakeFunction()
    {
        if (_bgmAudioSource == null)  Debug.LogError("BGM AudioSource is null."); //nullチェック
        if (_seAudioSource  == null)  Debug.LogError("SE AudioSource is null.");
    }
}

[System.Serializable]
public class BGMSoundData
{
    public enum BGM
    {
        Title, Dungeon, Hoge, // これがラベルになる
    }


    public BGM       _bgm;
    public AudioClip _audioClip;

    [Range(0, 1)]
    public float _volume = 1;
}

[System.Serializable]
public class SeSoundData
{
    public enum SE
    {
        Attack, Damage, Hoge // これがラベルになる
    }

    public SE        _se;
    public AudioClip _audioClip;

    [Range(0, 1)]
    public float _volume = 1;
}