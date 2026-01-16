using System.Collections;
using System.Collections.Generic;
using System;
using UnityEngine.SceneManagement;
using UnityEngine;

public class EndPoint : MonoBehaviour {

    //"Script by Brian Dang"
    // This Script is for reaching the endpoint of the track, where the player is sent to the results screen and records the time it took to reach the end.


    public float RaceTime;
    TimeSpan timeSpan; 

    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            PlayerPrefs.SetInt("RaceComplete", 1);
            string timeText = string.Format("{0:D2}:{1:D2}:{2:D2}", timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
            PlayerPrefs.SetString("RaceTime", timeText);
            SceneManager.LoadScene("Menu");

        }
    }

    void Update()
    {
        RaceTime += Time.deltaTime;
        timeSpan = TimeSpan.FromSeconds(RaceTime);
    }
}