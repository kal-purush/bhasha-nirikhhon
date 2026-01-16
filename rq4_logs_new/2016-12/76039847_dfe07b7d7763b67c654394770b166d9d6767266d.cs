using UnityEngine;
using System.Collections;

public class SoundContoller : MonoBehaviour 
{
	public AudioSource soundSource = null;
	private AudioClip[] soundClip = null;

	void Start () 
	{
		soundClip = Resources.LoadAll<AudioClip>("Sounds");

		soundSource.clip = soundClip [0];
		soundSource.Play ();
	}

	void OnEnable()
	{
		GameController.SectionEvent += Change1;
		GameController.SectionEndEvent += Change2;
	}

	void OnDisable()
	{
		GameController.SectionEvent -= Change1;
		GameController.SectionEndEvent -= Change2;
	}

	void Change1()
	{
		soundSource.clip = soundClip [1];
		soundSource.Play ();
	}

	void Change2()
	{
		soundSource.clip = soundClip [0];
		soundSource.Play ();
	}
}