using UnityEngine;
using System.Collections;

public class Mage : TowerClass {

	LineRenderer line;
	
	// Use this for initialization
	void Start () {

		base.Start ();
	
		line = gameObject.GetComponentInChildren<LineRenderer>();
		//line.SetPosition(0, GameObject.FindGameObjectWithTag("Mage"));
		//line.SetPosition(1, tower.)
	}

	// Update is called once per frame
	public override void Update () {

		if (!isFired) 
		{
			line.enabled = false;
		} 
		else 
		{
			line.enabled = true;
		}

		base.Update ();
	
	}

	public override void Shooting ()
	{
		isFired = true;
		
		lastShot = 0f;

		line.SetPosition (0, GameObject.FindGameObjectWithTag ("Mage").gameObject.transform.position);
		line.SetPosition (1, GetChosen().transform.position);

		if (GetChosen ().gameObject.tag == "Enemy")
			GetChosen().gameObject.GetComponent<AIBase> ().ApplyDamage (2);

	}
}