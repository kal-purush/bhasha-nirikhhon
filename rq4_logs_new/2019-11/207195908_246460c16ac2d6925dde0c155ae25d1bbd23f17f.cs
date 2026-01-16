using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HP_IMGUI : MonoBehaviour {
	public float health;
	public Slider hp;

	void Start()
	{
		health = 100.0f;
	}

	void OnGUI()
	{
		if (GUI.Button(new Rect(Screen.width / 2 - 30, 70, 60, 40), "回血"))
		{
			health = health < 100.0f ? health + 1.0f : 100.0f;
		}
		if (GUI.Button(new Rect(Screen.width / 2 - 30, 120, 60, 40), "扣血"))
		{
			health = health > 0.0f ? health - 1.0f : 0.0f;
		}

		GUI.HorizontalScrollbar(new Rect(Screen.width / 2 - 100, 20, 200, 40), 0.0f, health, 0.0F, 100.0F);
		hp.value = health;
	}
}