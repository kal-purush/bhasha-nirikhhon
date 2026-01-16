using UnityEngine;
using System.Collections;

public class HomeButton : MonoBehaviour {
	
	private AudioSource audioSource;
	private bool clicked = false;
	
	void Start()
	{
		audioSource = GetComponent<AudioSource> ();
	}
	
	void OnMouseDown()
	{
		clicked = true;
		audioSource.Play();
	}
	
	void Update()
	{
		if(!audioSource.isPlaying && clicked)
		{
			Application.LoadLevel ("Menu");
		}
	}
}