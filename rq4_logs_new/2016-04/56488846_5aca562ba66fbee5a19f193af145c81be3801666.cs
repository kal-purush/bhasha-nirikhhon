using UnityEngine;
using System.Collections;

public class Absorbing : TowerClass {

	LineRenderer line;
	public RailManager rail;

	// Use this for initialization
	protected override void Start () {

		gameObject.tag = "Absorbing";

		SetClassHealth (2);
		SetRadius (1);
		SetLevel (1);
		SetCooldown (2);
		SetHealth ();
		
		SetGoal(GameObject.FindGameObjectWithTag("Goal"));

		line = gameObject.GetComponentInChildren<LineRenderer>();

	}
	
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
		
		line.SetPosition (0, GameObject.FindGameObjectWithTag ("Absorbing").gameObject.transform.position);
		line.SetPosition (1, GetChosen().transform.position);
		
		if (GetChosen ().gameObject.tag == "Enemy") 
		{
			GetChosen ().gameObject.GetComponent<AIBase> ().ApplyDamage (10);
			rail.moveSpeed = rail.moveSpeed/2;
		}
	}
}