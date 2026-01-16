using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShipSpawner : MonoBehaviour {
	// Screen rectangle where ships are spawned.
	public Vector2 Distance;

	// Seconds between ships spawning.
	public float Period;
	public float PeriodRandom;

	// Minimum angle between successive ships.
	public float MinimumDeltaAngle;

	// Template for spawning ships.
	private GameObject ShipTemplate;

	// Time when next ship will spawn.
	private float NextShipTime;

	// Direction the next ship will appear at.
	private float NextShipAngle;

	void Start() {
		if (this.transform.childCount != 1) {
			Debug.LogError("ShipSpawner should have exactly one child");
			return;
		}
		this.ShipTemplate = this.transform.GetChild(0).gameObject;
		this.ShipTemplate.SetActive(false);
		this.NextShipTime = Time.time;
		this.NextShipAngle = Random.Range(-Mathf.PI, Mathf.PI);
	}
	
	void Update() {
		if (Time.time >= this.NextShipTime) {
			this.SpawnShip();
		}
	}

	void SpawnShip() {
		// Instantiate ship.
		float x = Mathf.Cos(this.NextShipAngle), y = Mathf.Sin(NextShipAngle);
		float r;
		if (Mathf.Abs(x) * this.Distance.y > Mathf.Abs(y) * this.Distance.x) {
			r = this.Distance.x / Mathf.Abs(x);
		} else {
			r = this.Distance.y / Mathf.Abs(y);
		}
		x *= r;
		y *= r;
		GameObject ship = Object.Instantiate<GameObject>(this.ShipTemplate, new Vector3(x, y), Quaternion.identity);
		ship.SetActive(true);

		// Reset timer for next ship.
		this.NextShipTime += Random.Range(this.Period - this.PeriodRandom, this.Period + this.PeriodRandom);
		this.NextShipAngle += Random.Range(this.MinimumDeltaAngle, 2.0f * Mathf.PI - this.MinimumDeltaAngle);
		if (this.NextShipAngle > Mathf.PI) {
			this.NextShipAngle -= 2.0f * Mathf.PI;
		}
	}
}