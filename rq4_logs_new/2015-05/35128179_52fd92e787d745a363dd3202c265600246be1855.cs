using UnityEngine;
using System.Collections;

public class music: MonoBehaviour {

	//An AudioSource object so the music can be played
	private AudioSource aSource;
	//A float array that stores the audio samples
	public float[] samples = new float[256];
	public ParticleSystem m_currentParticleEffect;
	private ParticleSystem.Particle[] ParticleList;


	void Awake () {
		//Get and store a reference to the following attached components:
		//AudioSource
		this.aSource = GetComponent<AudioSource> ();
		ParticleSystem m_currentParticleEffect = (ParticleSystem)GameObject.Find("Fireflies").GetComponent("ParticleSystem");

	}

	// Update is called once per frame
	void Update () {
		//Obtain the samples from the frequency bands of the attached AudioSource
		aSource.GetSpectrumData (this.samples, 0, FFTWindow.BlackmanHarris);
		float sum = 0;

		//For each sample
		for (int i=0; i<samples.Length; i++) {
			sum += samples [i] * samples [i];
		}
		float rms = Mathf.Sqrt (sum / 256f) * 50f;
		Debug.Log (rms*200);

		ParticleSystem.Particle []ParticleList = new ParticleSystem.Particle[m_currentParticleEffect.particleCount];
		m_currentParticleEffect.GetParticles(ParticleList);
		m_currentParticleEffect.emissionRate = (int)rms*50 + 5;
		
		for(int i = 0; i < ParticleList.Length; ++i)
		{
			ParticleList[i].size = rms*400+250;
			ParticleList[i].velocity = new Vector3(rms*300,0,0);
		}        
		m_currentParticleEffect.SetParticles(ParticleList, m_currentParticleEffect.particleCount);
	}
}