using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.AI;

public class EnemyController : MonoBehaviour {

	public float lookRadius = 10f;
	public GameObject[] players;
	public Transform target;

	private NavMeshAgent agent;

	// Use this for initialization
	void Start() {
		//target = PlayerManager.instance.player.transform;
		agent = GetComponent<NavMeshAgent>();
	}

	// Update is called once per frame
	void Update() {
		players = GameObject.FindGameObjectsWithTag("Player");

		if (players.Length == 0) { return; }

		//order players by distance
		players = players.OrderBy(player => Vector2.Distance(this.transform.position, player.transform.position)).ToArray();

		target = players.Last().transform;

		float distance = Vector3.Distance(target.position, transform.position);

		if (distance <= lookRadius) {
			agent.SetDestination(target.position);
			if (distance <= agent.stoppingDistance) {
				//Attack
				faceTarget();
			}
		}
	}

	void faceTarget() {
		Vector3 direction = (target.position - transform.position).normalized;
		Quaternion lookRotation = Quaternion.LookRotation(new Vector3(direction.x, 0, direction.z));
		transform.rotation = Quaternion.Slerp(transform.rotation, lookRotation, Time.deltaTime * 5f);
	}

	void OnDrawGizmosSelected() {
		Gizmos.color = Color.red;
		Gizmos.DrawWireSphere(transform.position, lookRadius);
	}
}