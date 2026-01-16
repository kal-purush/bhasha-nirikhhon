using UnityEngine;
using System.Collections;

public class PlayerShooting : MonoBehaviour {

	public ParticleSystem muzzleFlash; //Gun Muzzle Flash
	public Animator anim;
	bool shooting = false;
	public GameObject impactPrefab; //Bullet Particle Impact


	GameObject[] impacts;
	int currentImpact = 0;
	int maxImpacts = 5;

	// Use this for initialization
	void Start () {

		impacts = new GameObject[maxImpacts];
		for (int i = 0; i < maxImpacts; i++) 
		{
		
			impacts [i] = Instantiate (impactPrefab);
		}


		anim = GetComponentInChildren<Animator> ();

	}
	
	// Update is called once per frame
	void Update () {

		if (Input.GetButtonDown ("Fire1")) 
		{
			muzzleFlash.Play ();
			anim.SetTrigger ("Fire");
			shooting = true;
		}
			
	}


	void FixedUpdate()
	{
		if (shooting) 
		{
			shooting = false;
			RaycastHit hit;

			if(Physics.Raycast(transform.position,transform.forward,out hit, 50f))
			{
				impacts [currentImpact].transform.position = hit.point;
				impacts [currentImpact].GetComponent<ParticleSystem> ().Play ();
				if (++currentImpact >= maxImpacts)
					currentImpact = 0;
			}
		
		}
	}
}