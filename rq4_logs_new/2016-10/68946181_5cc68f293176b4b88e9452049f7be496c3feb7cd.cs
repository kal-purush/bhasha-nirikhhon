using UnityEngine;
using System.Collections;

public class EnemyAttack : MonoBehaviour {
	// AI VARIABLES:

	public float attackDistance = 1.0f;
	public float waitTime = 0.5f;
	private float distanceToPlayer;

	private GameObject player;
	public GameObject projectile;

	// START:
	void Start() {
		player = GameObject.FindGameObjectWithTag("Player");
	} // END START()

	// UPDATE:
	void Update() {
		distanceToPlayer = Vector2.Distance(transform.position, player.transform.position);

		//  If the distance between the mob and the player
		//  is less than or equal to the AttackDistance,
		//  the mob launches a projectile at the player.
		if (distanceToPlayer <= attackDistance) {
			Debug.Log(this.gameObject.name + " is now in range! Ready to shoot player!");
			transform.position = transform.position;

			waitTime -= Time.deltaTime;
			if (waitTime <= 0 && projectile) {
				Instantiate(projectile, transform.position, Quaternion.identity);
				waitTime = 0.5f;
			}
		}
	} // END UPDATE()

	public float GetAttackDistance ()
	{
		return attackDistance;
	}
} // END CLASS EnemyAttack