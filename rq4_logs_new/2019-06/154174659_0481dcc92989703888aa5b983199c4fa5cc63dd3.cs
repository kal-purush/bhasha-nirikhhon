using UnityEngine.Audio;
using System;
using UnityEngine;

public class AudioManager : MonoBehaviour {

	public Sound[] sounds;

	public static AudioManager instance;

	// Use this for initialization
	void Awake () {

		if (instance == null) {
			instance = this;
		} else {
			Destroy(gameObject);
			return;
		}

		DontDestroyOnLoad(gameObject);

		foreach (Sound s in sounds) {
			//Adiciona um audiosource no array presente no objeto
			s.source = gameObject.AddComponent<AudioSource>();
			s.source.clip = s.clip;

			//Controla volume e pitch no objeto
			s.source.volume = s.volume;
			s.source.pitch = s.pitch;
			s.source.loop = s.loop;
		}
	}

	void Start() {
		Play("Theme");
	}

	public void Play (string name) {
		Sound s = Array.Find(sounds, sound => sound.name == name);
		if (s == null) {
			Debug.Log("Sound " + name + " not found!");
			return;
		}
		s.source.Play();
	}
}