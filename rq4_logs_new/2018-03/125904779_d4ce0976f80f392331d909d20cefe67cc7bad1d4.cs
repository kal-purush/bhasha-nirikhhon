using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.AI;

public class EnemyAi : MonoBehaviour {
	[SerializeField] private Transform target;
	[SerializeField] private float viewField = 60f;
	[SerializeField] private float viewDistance = 10f;
	[SerializeField] private bool hasTarget = false;
	[SerializeField] private GameObject[] players;
	[SerializeField] private AudioClip sound;
	[SerializeField] private AudioSource audioSource;

	private NavMeshAgent agent;

	/// <summary>
	/// Start is called on the frame when a script is enabled just before
	/// any of the Update methods is called the first time.
	/// </summary>
	private void Start() {
		agent = this.GetComponent<NavMeshAgent>();
		audioSource = this.GetComponent<AudioSource>();
	}

	/// <summary>
	/// Update is called every frame, if the MonoBehaviour is enabled.
	/// </summary>
	private void Update() {
		target = getClosestPlayer();

		if (target && hasTarget) {
			faceTarget();
			agent.SetDestination(target.position);
			//Attack
		} else seekTarget();
	}

	/// <summary>
	/// If target exists, check if it´s inside view (field and distance) before lockTarget.
	/// </summary>
	private void seekTarget() {
		if (target)
			if (Vector3.Distance(this.transform.position, target.position)< viewDistance)
				if (Vector3.Angle(Vector3.forward, this.transform.InverseTransformPoint(target.position))< viewField)
					lockTarget();
	}

	/// <summary>
	/// Sets hasTarget to true and play sound.
	/// </summary>
	public void lockTarget() {
		hasTarget = true;
		audioSource.PlayOneShot(sound);
	}

	/// <summary>
	/// Adjust enemy rotation to face the target.
	/// </summary>
	private void faceTarget() {
		Vector3 direction = (target.position - transform.position).normalized;
		Quaternion lookRotation = Quaternion.LookRotation(new Vector3(direction.x, 0, direction.z));
		this.transform.rotation = Quaternion.Slerp(this.transform.rotation, lookRotation, Time.deltaTime * 5f);
	}

	/// <summary>
	/// Gets all GameObjects with tag "Player", order by distance and return first (closest?).
	/// </summary>
	private Transform getClosestPlayer() {
		players = GameObject.FindGameObjectsWithTag("Player");

		if (players.Length == 0)return null;
		else return players.OrderBy(player => Vector2.Distance(this.transform.position, player.transform.position)).ToArray().First().transform;
	}
}