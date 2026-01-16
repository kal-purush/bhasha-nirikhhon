using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Video;
using UnityEngine.SceneManagement;

[RequireComponent(typeof(AudioSource))]
public class startmoive : MonoBehaviour
{
    private VideoPlayer vPlayer;
    private AudioSource source;
    void Start()
    {

    }
    private void Awake()
    {
        vPlayer = GetComponent<VideoPlayer>();
        source = GetComponent<AudioSource>();
    }

    // Update is called once per frame
    void Update()
    {



    }

    void OnGUI()
    {
        //播放完毕
        if ((int)vPlayer.time >= (int)GetComponent<VideoPlayer>().clip.length)
        {
            SceneManager.LoadScene("MAINUI");
        }
        else if(Input.GetKeyDown(KeyCode.Escape))
        {
            SceneManager.LoadScene("MAINUI");

        }
    }
}