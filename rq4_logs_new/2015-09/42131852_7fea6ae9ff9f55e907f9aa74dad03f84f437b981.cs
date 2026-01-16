using UnityEngine;
using System.Collections;

public class ZombieAttack : MonoBehaviour
{
	
	private float distance;
	private float maxdistance = 1.2f;
	public Vector3 offset;
	
	// Use this for initialization
	void FixedUpdate()
	{

		if (Input.GetKey(KeyCode.X))
		{
			//Test to see if enemy is in range, if so take it takes damage while kicking.
			RaycastHit hit;
			Debug.DrawLine(offset + transform.position, transform.right * 100, Color.green);
			if (Physics.Raycast(offset + transform.position, transform.right, out hit))
			{
				distance = hit.distance;
				print(distance);
				print(hit.transform.tag);
				if (distance < maxdistance && hit.transform.tag == "Enemy")
				{
					GameObject enemyhit = hit.transform.gameObject;
					enemyhit.GetComponent<Enemy>().init_health--;
				}
			}
				
		}

	}
}