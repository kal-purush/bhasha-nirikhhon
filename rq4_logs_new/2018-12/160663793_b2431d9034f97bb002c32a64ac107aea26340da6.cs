using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TestPlayerHealth : MonoBehaviour
{

	public int PlayerHealth; 
	public int NumOfHealth; //Number of health bars
	public Image[] HealthBars;
	public Sprite RedBars; //Red health
	public Sprite BlueBars;	//Blue shield bars

	
	void Update () {

		if (PlayerHealth > NumOfHealth)
		{
			PlayerHealth = NumOfHealth;
		}
		
		for (int i = 0; i < HealthBars.Length; i++)
		{
			if (i < NumOfHealth)
			{
				HealthBars[i].enabled = true; 
			}
			else
			{
				HealthBars[i].enabled = false;
			}
		}
		
	}
}