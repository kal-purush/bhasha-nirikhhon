using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;
using static UnityEngine.UIElements.UxmlAttributeDescription;
using System.Linq;
using System;

public class AudioController : MonoBehaviour
{
    //Variables accessible depuis l'inspecteur
    [SerializeField] List<AudioClip> backgroundMusics;
    [SerializeField] float backgroundVolume;
    [SerializeField] private AudioClip _pointClip;
    [SerializeField] private float _pointVolume;
    [SerializeField] private AudioClip _explosionClip;
    [SerializeField] private float _explosionVolume;
    [SerializeField] private AudioClip _attackClip;
    [SerializeField] private float _attackVolume;
    [SerializeField] private AudioClip _looseLifeClip;
    [SerializeField] private float _looseLifeVolume;
    [SerializeField] private AudioClip _gameOverClip;
    [SerializeField] private float _gameOverVolume;
    [SerializeField] private AudioClip _jumpClip;
    [SerializeField] private float _jumpVolume;
    [SerializeField] private AudioClip _confirmClip;
    [SerializeField] private float _confirmVolume;



    private AudioSource backgroundAudio;




    void Start()
    {
        //création de l'index
        int randomIndex;
        if (!PlayerPrefs.HasKey("MusicIndex"))
        {
            UnityEngine.Random.InitState(System.DateTime.Now.Millisecond);
            randomIndex = UnityEngine.Random.Range(0, 2);
            Debug.Log("index new" + randomIndex.ToString());
            PlayerPrefs.SetInt("MusicIndex", randomIndex);
        }
        else
        {
            randomIndex = PlayerPrefs.GetInt("MusicIndex");
        }

        //creation du timer
        if (!PlayerPrefs.HasKey("MusicTime"))
        {
            PlayerPrefs.SetFloat("MusicTime", 0f);
        }

        //Cr�ation d'un composant audio pour chaque type de sons
        backgroundAudio = gameObject.AddComponent<AudioSource>();

        //Ajustement des volumes
        backgroundAudio.volume = backgroundVolume;


        StartCoroutine(PlayBackground(randomIndex));
    }


    private void OnApplicationQuit()
    {
        PlayerPrefs.DeleteKey("MusicIndex");
        PlayerPrefs.DeleteKey("MusicTime");
    }

    public void AudioUpdate()
    {
        PlayerPrefs.SetFloat("MusicTime", backgroundAudio.time);
    }


    public IEnumerator PlayBackground(int currentIndex)
    {
        AudioClip music = backgroundMusics[currentIndex % backgroundMusics.Count()];
        Debug.Log(currentIndex % (backgroundMusics.Count() - 1));
        
        backgroundAudio.clip = music;
        backgroundAudio.time = PlayerPrefs.GetFloat("MusicTime");
        backgroundAudio.Play();

        yield return new WaitForSeconds(music.length);
        
        StartCoroutine(PlayBackground(currentIndex++));
    }


    public void PlayPoint()
    {
        StartCoroutine(PlaySound(_pointClip, _pointVolume));
    }

    public void PlayAttack()
    {
        StartCoroutine(PlaySound(_attackClip, _attackVolume));
    }

    public void PlayJump()
    {
        Debug.Log("PlayJump");
        StartCoroutine(PlaySound(_jumpClip, _jumpVolume));
    }

    public void PlayGameOver()
    {
        StartCoroutine(PlaySound(_gameOverClip, _gameOverVolume));
    }

    public void PlayLooseLife()
    {
        StartCoroutine(PlaySound(_looseLifeClip, _gameOverVolume));
    }

    public void PlayExplosion()
    {
        StartCoroutine(PlaySound(_explosionClip, _explosionVolume));
    }

    public float PlayConfirm()
    {
        StartCoroutine(PlaySound(_confirmClip, _confirmVolume));
        return _confirmClip.length;
    }

    public IEnumerator PlaySound(AudioClip clip, float volume)
    {
        AudioSource tempAudio = gameObject.AddComponent<AudioSource>();
        tempAudio.clip = clip;
        tempAudio.volume = volume;

        tempAudio.Play();
        yield return new WaitForSeconds(tempAudio.clip.length);
        Destroy(tempAudio);
    }
}