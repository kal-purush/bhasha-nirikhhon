using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ARsound : MonoBehaviour {
	public AudioClip BulletClip;
	public AudioSource BulletSource;
	public static ARsound soundManager;

	void Awake(){
		if (ARsound.soundManager == null) {
			ARsound.soundManager = this;
		}
	}
	void Start(){
		BulletSource = GetComponent<AudioSource> ();
	}
	public void PlaySound(){
		BulletSource.PlayOneShot (BulletClip);
	}
}