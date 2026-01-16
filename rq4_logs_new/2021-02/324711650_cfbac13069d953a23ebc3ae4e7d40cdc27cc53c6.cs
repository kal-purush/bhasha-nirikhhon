using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.Audio;
using UnityEngine.UI;

/*
 * 환경설정 (Setting Scene)과 관련된 코드
 * - 슬라이더를 통한 볼륨 조절
 * - 볼륨값 저장 및 호출
 */

public class SoundController : MonoBehaviour
{
    public Slider bgSlider;             // 배경음악 Slider
    public Slider sfxSlider;            // 효과음 Slider
    private float bgSlideVal;           // 배경음악 볼륨값
    private float sfxSlideVal;          // 효과음 볼륨값


    // (배경음악) SoundManager의 instance 호출 - Slider에 적용
    public void BGsoundSlider(float value)
    {
        SoundManager.instance.BGSoundVolume(value);
        PlayerPrefs.SetFloat("bgVol", value);
    }


    // (효과음) SoundManager의 instance 호출 - Slider에 적용
    public void SFXSoundSlider(float value)
    {
        //SoundManager.instance.SFXSoundVolume(value);
        PlayerPrefs.SetFloat("sfxVol", value);
    }


    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        // 유저가 조정한 Slider의 값을 저장, 적용
        bgSlideVal = PlayerPrefs.GetFloat("bgVol", bgSlideVal);     // 배경음악 Slider의 값을 PlayerPrefs에 저장
        bgSlider.value = bgSlideVal;                                // PlayerPrefs에 저장된 값을 Slider에 적용

        sfxSlideVal = PlayerPrefs.GetFloat("sfxVol", sfxSlideVal);     // 배경음악 Slider의 값을 PlayerPrefs에 저장
        sfxSlider.value = sfxSlideVal;                                // PlayerPrefs에 저장된 값을 Slider에 적용
    }
}