using UnityEngine;
using System.Collections;

public class AI : MonoBehaviour {

	private Transform enemy;
	private SpawnManager spawnManager;
	public float moveSpeed;

	private void Start(){
		enemy = GameObject.FindGameObjectWithTag("Player").transform;
	}

	public void Set(GameObject mainGO){
		spawnManager = mainGO.GetComponent<SpawnManager>();
	}

	private void Update(){
		transform.LookAt (enemy);
		transform.position = Vector3.MoveTowards (transform.position, enemy.position, Time.deltaTime * moveSpeed);
	}


	public void Kill(){
		spawnManager.EnemyKilled();
		Destroy (this.gameObject);
	}
}