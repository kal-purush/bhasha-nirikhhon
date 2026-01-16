using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CLobbySliderScript : MonoBehaviour
{
    public Slider playerSlider;
    private int nowPlayer;
    private int maxPlayer;

    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        playerSlider.value = nowPlayer;
        playerSlider.maxValue = maxPlayer;
    }

    public void SetPlayerNumber(int _PlayerNumber, int _Maxplayer)
    {
        nowPlayer = _PlayerNumber;
        maxPlayer = _Maxplayer;
    }
}