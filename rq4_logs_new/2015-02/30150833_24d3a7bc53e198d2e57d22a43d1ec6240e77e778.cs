using UnityEngine;
using UnityEngine.UI;
using System.Collections;

using ThinksquirrelSoftware.Fluvio;
using ThinksquirrelSoftware.Fluvio.ObjectModel;

public class GameplayManager : MonoBehaviour
{
	public UnityEngine.UI.Text textTimer;
	public UnityEngine.UI.Text textParticlesInfo;

	public Fluid fluid;
	public ColliderParticleCounter cpc;
	
	private float time = 20.0f;
	private int startParticles = -1;
	private FluidParticle[] particles;
	
	void OnEnable()
	{
		fluid.OnPostSolve += OnPostSolve;
	}
	
	void OnDisable()
	{
		fluid.OnPostSolve -= OnPostSolve;
	}
	
	void OnPostSolve(FluvioTimeStep timeStep)
	{
		fluid.GetParticles(ref particles);
	}

	public void RunStage()
	{
		startParticles = cpc.fluidParticlesCounter;
	}

	// Use this for initialization
	void Start ()
	{
	
	}
	
	// Update is called once per frame
	void Update ()
	{
		if (startParticles == -1)
			return; //game not started

		time -= Time.deltaTime;
		if (time <= 0.0f || cpc.fluidParticlesCounter == 0 || fluid.activeParticleCount == 0)
			StopStage ();

		textTimer.text = string.Format ("Time: {0} seconds", time);
		textParticlesInfo.text = string.Format("Total particles: {0}\nParticles in cup: {1}\nParticles in saucer: {2}", fluid.activeParticleCount, cpc.fluidParticlesCounter, fluid.activeParticleCount - cpc.fluidParticlesCounter);
	}

	private void StopStage()
	{
		if (cpc.fluidParticlesCounter > startParticles / 2)
			Application.LoadLevel ("goodFinalScene");
		else
			Application.LoadLevel ("badFinalScene");
	}
}