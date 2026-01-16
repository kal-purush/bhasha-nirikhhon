using UnityEngine;
using System.Collections;

public class SpawnEnemy : MonoBehaviour {

	public GameObject[] enemy;
	public float spawnWait;
	public float spawnMostWait;
	public float spawnLeastWait;
	public int startWait;
	public bool stop;

	int randEnemy;

	void Start ()
	{
		StartCoroutine (waitSpawner());
	}

	void Update () 
	{
		spawnWait = Random.Range (spawnLeastWait, spawnMostWait);
	}

	public IEnumerator waitSpawner()
	{
		yield return new WaitForSeconds (startWait);

		while (!stop)
		{
			randEnemy = Random.Range (0, 4);
			Instantiate (enemy[randEnemy], transform.position, Quaternion.identity);

			yield return new WaitForSeconds (spawnWait);
		}
	}
}