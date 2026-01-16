using UnityEngine;
using System.Collections;

public class GhostHypnotize : MonoBehaviour {

	private GameObject mytarget;
	private const int maxdistance = 10;
	
	void Update () 
	{
		if (Input.GetKey(KeyCode.X))
		{
			this.mytarget = this.findClosestEnemy();
			this.mytarget.GetComponentInChildren<Enemy>().moveSpeed = -3;
			LineRenderer lineRenderer = this.GetComponentInChildren<LineRenderer>();
			lineRenderer.SetPosition(0, this.transform.position);
			lineRenderer.SetPosition(1, this.mytarget.transform.position);
		}
	}

	private GameObject findClosestEnemy()
	{
		GameObject[] enemies;
		enemies = GameObject.FindGameObjectsWithTag("Enemy");
		GameObject closest = null;
		float distance = Mathf.Infinity;
		Vector3 position = transform.position;

		foreach (GameObject enemy in enemies) 
		{
			Vector3 diff = enemy.transform.position - position;
			float curDistance = diff.sqrMagnitude;
			if (curDistance < distance) 
			{
				closest = enemy;
				distance = curDistance;
			}
		}
		return closest;
	}
}