using UnityEngine;
using System.Collections;

public class RainController : MonoBehaviour {

	private SeasonController seasonCtrl;

	private ParticleSystem particles;

	// Use this for initialization
	void Start () {
		seasonCtrl = (SeasonController) FindObjectOfType (typeof(SeasonController));

		particles = GetComponent<ParticleSystem> ();
	}
	
	// Update is called once per frame
	void Update () {
		if (seasonCtrl.season == "spring") {
			particles.emissionRate = 75;
		} else {
			particles.emissionRate = 0;
		}
	}
}