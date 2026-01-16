using UnityEngine;
using System.Collections;

public class ZombieAttack : MonoBehaviour {
	
	private float distance;
	private float maxdistance = 1.2f;
	public Vector3 offset;
	int EnemyLayer = 1 << 8;
	LayerMask;
	
	// Use this for initialization
	void FixedUpdate () {
		
		if(this.GetComponent<Movement>().isActivePlayer)
		{
			if (Input.GetKey(KeyCode.X))
			{
				
				
				//Test to see if enemy is in range, if so take it takes damage while kicking.
				RaycastHit hit;
				Debug.DrawLine (offset + transform.position, transform.right*100, Color.green);
				if(Physics.Raycast(offset +transform.position, transform.right, out hit, EnemyLayer)) 
				{
					distance = hit.distance;
					print(distance);
					if(distance<maxdistance)
					{
						GameObject enemyhit = GameObject.Find("Enemy");
						enemyhit.GetComponent<Enemy>().init_health--;
					}
				}
				
			}
		}
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}