using UnityEngine;
using System.Collections;

public class PressAnyKey : MonoBehaviour 
{
	public GameObject title;
	public GameObject selectionFollow;
	public GameObject selection;
	public GameObject playButton;
	public GameObject optionsButton;
	public GameObject creditsButton;
	public GameObject exitButton;

	private float counterAnyKey = 1;
	private float counterMenu = 0;
	private float counterTitle = 0;
	private bool deactiveCounter = false;
	private bool activeCounter = false;

	void Update () 
	{
		if(Input.anyKey)
		{
			deactiveCounter = true;
		}
		if (deactiveCounter == true)
		{
			counterAnyKey -= 0.01f;
			GetComponent<Renderer> ().material.color = new Color(1, 1, 1, counterAnyKey);
			if(counterAnyKey <= 0)
			{
				activeCounter = true;
				Debug.Log (activeCounter);
				this.gameObject.SetActive(false);
			}
		}

		if(activeCounter == true)
		{
			counterTitle += 0.01f;
			Debug.Log(counterTitle);
			counterMenu += 0.005f;
			Debug.Log(counterMenu);

			Debug.Log (activeCounter);

			title.SetActive(true);
			title.GetComponent<Renderer> ().material.color = new Color(1, 1, 1, counterTitle);
			selectionFollow.SetActive(true);
			selectionFollow.GetComponent<Renderer> ().material.color = new Color(1, 1, 1, counterMenu);
			selection.SetActive(true);
			selection.GetComponent<Renderer> ().material.color = new Color(1, 1, 1, counterMenu);
			playButton.SetActive(true);
			playButton.GetComponent<Renderer> ().material.color = new Color(1, 1, 1, counterMenu);
			optionsButton.SetActive(true);
			optionsButton.GetComponent<Renderer> ().material.color = new Color(1, 1, 1, counterMenu);
			creditsButton.SetActive(true);
			creditsButton.GetComponent<Renderer> ().material.color = new Color(1, 1, 1, counterMenu);
			exitButton.SetActive(true);
			exitButton.GetComponent<Renderer> ().material.color = new Color(1, 1, 1, counterMenu);
		}
	}
}