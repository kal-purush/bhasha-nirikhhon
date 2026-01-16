using UnityEngine;
using System.Collections;

public class ParticleManager : MonoBehaviour {

	public static ParticleManager _instance;

	public ParticleSystem personDeathParticleLava;
	public ParticleSystem personDeathParticleSmoke;

	// Use this for initialization
	void Awake () {
		_instance=this;
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	public void playBurnPersonParticles(){
		personDeathParticleLava.Play();
		personDeathParticleSmoke.Play();
	}
}