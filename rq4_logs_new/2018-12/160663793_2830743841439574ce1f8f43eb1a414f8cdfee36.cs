using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;


public class ButtonManagerTest : MonoBehaviour
{

	public GameObject OptionsPanel;
	public GameObject GameplayPanel;
	public GameObject GraphicsPanel;
	public GameObject SoundPanel;
	

	// Use this for initialization
	void Start ()
	{
		OptionsPanel.SetActive(false);
	}
	
	// Update is called once per frame
	void Update () {
		
	}

	public void Options()
	{
		OptionsPanel.SetActive(true);
	}
	
	public 
}