using UnityEngine;
using System.Collections;

public class SoundManager : MonoBehaviour {

	public static SoundManager instance;

	public bool muted = false;

	public AudioSource Efx;

	public AudioClip uiSound;

	public float lowPitchRange = 0.95f;
	public float highPitchRange = 1.05f;

	// Use this for initialization
	void Awake () {
		// Make this a singleton
		if (instance == null)
			instance = this;
		else if (instance != this)
			Destroy (gameObject);
		
		DontDestroyOnLoad(gameObject);
	}

	void Update()
	{
		Efx.mute = muted;
	}

	public void RandomizeSfx(AudioClip clip)
	{
		// randomize the pitch of each sound a little bit
		float randomPitch = Random.Range (lowPitchRange, highPitchRange);
		Efx.pitch = randomPitch;
		Efx.clip = clip;
		Efx.Play ();
	}

	public void PlaySingle(AudioClip clip)
	{
		Efx.clip = clip;
		Efx.Play ();
	}

	public void PlayUISound()
	{
		Efx.clip = uiSound;
		Efx.Play ();
	}

}