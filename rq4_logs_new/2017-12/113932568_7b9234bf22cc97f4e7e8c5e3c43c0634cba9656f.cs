using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class MainUIManager : MonoBehaviour {

    public Text money;
    public Text day;
    public Text place;

    public Button dance;
    public Button sound;
    public Button coordi;

    public GameObject dancePanel;
    public GameObject soundPanel;
    public GameObject coordiPanel;

	// Use this for initialization
	void Start () {
        dancePanel.SetActive(true); // dance default
        soundPanel.SetActive(false);
        coordiPanel.SetActive(false);
	}

    void Update()
    {
        money.text = PlayerData.getInstance().money.ToString();
        day.text = "DAY. " + PlayerData.getInstance().day;
    }

    public void onClick_Dance()
    {
        panelActive(dancePanel, soundPanel, coordiPanel);

    }

    public void onClick_Sound()
    {
        panelActive(soundPanel, dancePanel, coordiPanel);
    }

    public void onClick_Coordi()
    {
        panelActive(coordiPanel, dancePanel, soundPanel);
    }

    /// <summary>
    /// 패널 활성화
    /// </summary>
    /// <param name="active">활성화</param>
    /// <param name="panel1">비활</param>
    /// <param name="panel2">비활</param>
    public void panelActive(GameObject active, GameObject panel1, GameObject panel2)
    {
        active.SetActive(true);
        panel1.SetActive(false);
        panel2.SetActive(false);
    }
}