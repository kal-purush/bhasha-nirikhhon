using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BGMManager : MonoBehaviour
{
    [SerializeField, Header("BGM")]
    AudioSource[] bgms;
    [SerializeField, Header("SE")]
    AudioClip[] se;

    public void PlayBGM(int id)
    {
        bgms[id].Play();
    }

    public void StopBGM()
    {
        foreach (var bgm in bgms)
        {
            if (bgm.isPlaying)
                bgm.Stop();
        }
    }

    public void PlaySE(int id, Vector3 pos)
    {
        AudioSource.PlayClipAtPoint(se[id], pos);
    }
}