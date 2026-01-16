using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class MenuScene : MonoBehaviour {

    public bool creditShow;
    public GameObject creditPanel;

	// Use this for initialization
	void Start () {
        creditPanel.SetActive(creditShow);
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    public void StartGame()
    {
        SceneManager.LoadScene(1);
    }

    public void QuitGame()
    {
        Application.Quit();
    }

    public void TriggerCredits()
    {
        creditShow = !creditShow;
        creditPanel.SetActive(creditShow);
    }
}