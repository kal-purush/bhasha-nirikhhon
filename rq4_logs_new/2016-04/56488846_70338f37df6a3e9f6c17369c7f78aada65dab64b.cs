using UnityEngine;
using System.Collections;

public class Ballistics : TowerClass {

	Animation anim;

	// Use this for initialization
	protected override void Start () {

		anim = GetComponentInChildren<Animation> ();

		SetClassHealth (4);
		SetRadius (0);
		SetLevel (1);
		SetCooldown (1);
		SetHealth ();
		
		SetGoal(GameObject.FindGameObjectWithTag("Goal"));
	
	}
	
	// Update is called once per frame
	public override void Update () {
	
		speed = GetLevel () * 2;

		if (!isFired)
			anim.Stop ();

		base.Update ();

	}

	public override void Shooting()
	{
		anim.Play ();
		base.Shooting ();
	}
}