using UnityEngine;
using System.Collections;

using ThinksquirrelSoftware.Fluvio;
using ThinksquirrelSoftware.Fluvio.ObjectModel;

public class ParticleCounter : MonoBehaviour
{
	public Fluid fluid;

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

	// Use this for initialization
	void Start ()
	{
	
	}
	
	// Update is called once per frame
	void Update ()
	{
		guiText.text = string.Format("Total particles: {0}\nParticles in cup: {1}\nParticles in saucer: {2}", fluid.activeParticleCount, 0, 0);
	}
}