using UnityEngine;
using System.Collections;

public class TowerClass : MonoBehaviour {

	public GameObject ai;

	public GameObject bullet;

	public GameObject spawnPoint;

	int health = 100;

	int damage = 5;

	float fireRate = 3f;

	public bool upgradable;

	float radius = 12f;
	
	public int level;

	float rotateSpeed = 1f;

	public float lastShot = 0f;

	public float cooldown = 3f;

	bool isFired = false;

	Vector3 direction = new Vector3(0,0,0);

	// Update is called once per frame
	void Update () {
	
		if (lastShot > cooldown) 
		{
			isFired = false;
		}

		if (HasEnemyInSight (ai) && !isFired) 
		{
			Shooting ();
		}

		lastShot += Time.deltaTime;

	}

	bool HasEnemyInSight(GameObject enemy)
	{
		if (Vector3.Distance (gameObject.transform.position, enemy.gameObject.transform.position) < radius) 
		{
			direction = enemy.gameObject.transform.position - gameObject.transform.position;

			Quaternion rotate = Quaternion.LookRotation(direction);
			rotate.x = 0;
			rotate.z = 0;

			gameObject.transform.rotation = Quaternion.Slerp(gameObject.transform.rotation, rotate, Time.deltaTime * rotateSpeed);

			return true;
		}
		return false;
	}

	virtual public void Shooting()
	{
		isFired = true;

		lastShot = 0f;

		Vector3 position = gameObject.transform.position;
		GameObject go = Instantiate (bullet) as GameObject;
		go.transform.position = spawnPoint.transform.position;
		go.transform.rotation = spawnPoint.transform.rotation;
		go.AddComponent<Rigidbody> ();
		go.GetComponent<Rigidbody> ().velocity = direction;
	}
}