using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GolemAbility : MonoBehaviour {

	public float lifetime = 2f;
	public float timeTillCharge = .5f;

	[Space(10)]

	public float startingChargeSpeed = 100f;
	public float chargeSlowRate = 1f;

	public float growthRate = .02f;

	private bool charging = false;

	void Start () 
	{
		StartCoroutine("ChargeTimer", timeTillCharge);
	}

	private IEnumerator ChargeTimer(float time)
	{
		float timer = 0;

		while(!charging)
		{
			transform.localScale += new Vector3(growthRate, growthRate, 0f);
			timer += Time.deltaTime;

			if(timer >= time)
				charging = true;
			yield return null;
		}

		StartCoroutine("ChargeHandler");
	}

	private IEnumerator ChargeHandler()
	{
		charging = true;
		while(charging)
		{
			transform.Translate(Vector3.up * Time.deltaTime * startingChargeSpeed);
			startingChargeSpeed -= chargeSlowRate;

			if(startingChargeSpeed <= startingChargeSpeed * .75f)
				chargeSlowRate *= 2;
			else if(startingChargeSpeed <= startingChargeSpeed * .5f)
				chargeSlowRate *= 2;
			else if(startingChargeSpeed <= startingChargeSpeed * .25f)
				chargeSlowRate *= 2;

			if(startingChargeSpeed <= 0)
			{
				charging = false;
			}
			yield return null;
		}

		yield return null;
	}

	private void OnTriggerEnter2D(Collider2D col)
	{
		if(col.gameObject.tag == "Boss")
		{
			//Do Damage
		}
	}
}