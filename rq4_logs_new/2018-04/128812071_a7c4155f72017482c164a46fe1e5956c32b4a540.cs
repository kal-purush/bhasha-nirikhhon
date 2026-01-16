using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Chat : MonoBehaviour {
    public Button Send;
    public Button Game1;
    public Button Game2;
    public Button History;
    public Button Picture;

    // Use this for initialization
    void Start ()
    {
		Send.onClick.AddListener(send);
        Game1.onClick.AddListener(game1);
        Game2.onClick.AddListener(game2);
        History.onClick.AddListener(history);
        Picture.onClick.AddListener(picture);

    }
	
	// Update is called once per frame
	void Update () {
		
	}

    public void send()
    {
        //
    }

    public void game1()
    {
        //
    }
    public void game2()
    {
        //
    }
    public void history()
    {
        //
    }
    public void picture()
    {
        //
    }
}