using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class enemyAI : MonoBehaviour
{

	public float moveSpeed;
	public float chaseSpeed;
	public float patrolRange;
	public float chaseRange;
	public float attackRange;
	public float maxChaseRange;

	public float attackForce;

	public enum EnemyState
	{
		Patroling,
		Chasing,
		Attacking

	}

	public EnemyState currentState;

	public GameObject Fish1;
	public GameObject Fish2;

	GameObject targetFish;

	Vector3 moveDestination;
	Vector3 moveDirection;

	Vector3 homePos;

	// Use this for initialization
	void Start()
	{

		currentState = EnemyState.Patroling;

		homePos = transform.position;

	}

	// Update is called once per frame
	void Update()
	{

		Vector3 fishPos1 = Fish1.transform.position;
		Vector3 fishPos2 = Fish2.transform.position;

		float disFish1 = Vector3.Magnitude(transform.position - fishPos1);
		float disFish2 = Vector3.Magnitude(transform.position - fishPos2);

		if (currentState == EnemyState.Patroling)
		{
			print("Enemy patroling");

			targetFish = null;

			if (Vector3.Magnitude(transform.position - moveDestination) <= 0.1f)
			{
				StartCoroutine(ranGenDest());
			}

			float disHom = Vector3.Magnitude(transform.position - homePos);
			if (disHom <= patrolRange)
			{
				if (disFish1 <= chaseRange || disFish2 <= chaseRange)
				{
					currentState = EnemyState.Chasing;
				}
			}

			moveDirection = moveDestination - transform.position;

			transform.position += moveDirection.normalized * moveSpeed * Time.deltaTime;
		}
		else if (currentState == EnemyState.Chasing)
		{

			print("Enemy chasing");

			if (targetFish == null)
			{
				if (disFish1 < disFish2)
				{
					targetFish = Fish1;
				}
				else if (disFish2 < disFish1)
				{
					targetFish = Fish2;
				}

			}
			else {
				ChaseTarget(targetFish);
			}


		}
		else if (currentState == EnemyState.Attacking)
		{
			print("Enemy attacking");

			StartCoroutine(PushTarget(targetFish));

		}

		if (moveDirection.x >= 0f)
		{
			transform.up = new Vector3(1, 0, 0);
		}
		else {
			transform.up = new Vector3(-1, 0, 0);
		}

	}

	IEnumerator ranGenDest()
	{
		moveDestination = homePos + new Vector3(Random.Range(-patrolRange, patrolRange), Random.Range(-patrolRange, patrolRange), 0f);
		yield return moveDestination;
	}

	void ChaseTarget(GameObject target)
	{
		moveDestination = target.transform.position;
		moveDirection = moveDestination - transform.position;
		transform.position += moveDirection.normalized * chaseSpeed * Time.deltaTime;

		float disTar = Vector3.Magnitude(target.transform.position - transform.position);
		float disHom = Vector3.Magnitude(transform.position - homePos);
		if (disTar < attackRange)
		{
			currentState = EnemyState.Attacking;
			targetFish = target;

		}
		else if (disHom > maxChaseRange)
		{
			currentState = EnemyState.Patroling;
			targetFish = null;
		}

	}

	IEnumerator PushTarget(GameObject target)
	{
		target.GetComponent<Rigidbody2D>().AddForce(moveDirection.normalized * attackForce);
		yield return new WaitForSeconds(1.5f);
		currentState = EnemyState.Patroling;
		targetFish = null;
	}



}