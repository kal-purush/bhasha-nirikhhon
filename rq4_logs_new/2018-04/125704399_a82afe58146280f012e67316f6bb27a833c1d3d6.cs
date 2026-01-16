using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MainModeFullScream : MonoBehaviour {

    public Transform myprogress;
    // Use this for initialization
    void Start()
    {
        //  Handheld.PlayFullScreenMovie("startcg.mp4", Color.black, FullScreenMovieControlMode.CancelOnInput);
    }

    // Update is called once per frame
    void Update()
    {
        gameObject.GetComponent<RectTransform>().sizeDelta = new Vector2(Screen.width, Screen.height);

    }

    void Awake()
    {
        gameObject.GetComponent<RectTransform>().sizeDelta = new Vector2(Screen.width, Screen.height);
        
    }

    void OnGUI()
    {

    }



}