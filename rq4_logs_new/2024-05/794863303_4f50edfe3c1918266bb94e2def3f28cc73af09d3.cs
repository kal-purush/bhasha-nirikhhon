using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Audio;
using UnityEngine.Serialization;

public class SoundManager : BaseSingleton<SoundManager>
{
    [SerializeField] AudioMixer  _audioMixer;
    [SerializeField] AudioSource _bgmAudioSource;
    [SerializeField] AudioSource _seAudioSource;


    protected override void AwakeFunction()
    {
        if (_bgmAudioSource == null)  Debug.LogError("BGM AudioSource is null."); //nullチェック
        if (_seAudioSource  == null)  Debug.LogError("SE AudioSource is null.");
    }
    /// <summary>
    /// BGMを再生
    /// </summary>
    /// <param name="audioClip">BGM</param>
    public void PlayBGM(AudioClip audioClip)
    {
        _bgmAudioSource.clip   = audioClip;
        _bgmAudioSource.Play();
    }
    /// <summary>
    /// SEを再生
    /// </summary>
    /// <param name="audioClip">Se</param>
    public void PlaySe(AudioClip audioClip) { _seAudioSource.PlayOneShot(audioClip); }

    public void MuteBGM(bool mute) { _bgmAudioSource.mute = mute; }
    public void MuteSE(bool  mute) { _seAudioSource.mute  = mute; }

    public void SetVolumeBGM(float volume) { _audioMixer.SetFloat("BGM", volume); }
}