using UnityEngine;
using System.Collections;

public class AutoProjectile : MonoBehaviour {

	public ShipStats shipStats;


	public float GetDamage ()
	{
		return shipStats.autoLaserDamage;
	}

	public void Hit ()
	{
		Destroy(gameObject);
	}
}