using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System;

public class MainBKFullScrean : MonoBehaviour
{
    public Transform myprogress;
    private Image img;
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
        System.Random rd = new System.Random();
        int i=rd.Next(0, 5);
        img = gameObject.GetComponent<Image>();
        Texture2D aa = Resources.Load<Texture2D>("loadbackimg/"+i.ToString()) as Texture2D;
        img.sprite = Sprite.Create(aa, new Rect(0, 0, aa.width, aa.height), new Vector2(1f, 1f));
       gameObject.GetComponent<RectTransform>().sizeDelta = new Vector2(Screen.width, Screen.height);
        Vector3 v3;
        if (i == 0||i==3||i==4)
        {
            v3 = new Vector3(0+myprogress.GetComponent<RectTransform>().sizeDelta.x/2, myprogress.GetComponent<RectTransform>().sizeDelta.y / 2, 0);
        }
        else if(i==1)
        {
            v3 = new Vector3(Screen.width / 3+ myprogress.GetComponent<RectTransform>().sizeDelta.x / 2, myprogress.GetComponent<RectTransform>().sizeDelta.y / 2, 0);
        }
        else
        {
            v3 = new Vector3(Screen.width / 3 * 2+ myprogress.GetComponent<RectTransform>().sizeDelta.x / 2, myprogress.GetComponent<RectTransform>().sizeDelta.y / 2, 0);

        }
        myprogress.position = v3;
       // setText.position = v3;
        //loadingtext.position = v3;

    }

    void OnGUI()
    {

    }

}