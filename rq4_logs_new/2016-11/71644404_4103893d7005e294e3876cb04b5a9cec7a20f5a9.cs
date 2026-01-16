using UnityEngine;
using System.Collections;

public class EnemySpawner : MonoBehaviour {

	/// <summary>
	/// Enemy prefab for spawning
	/// </summary>
	public Transform enemyPrefab;

	public float SpawnRate = 2.0f;
	private float deltaSpawn;
	private SpriteRenderer sr;

	// Use this for initialization
	void Start () {
		deltaSpawn = SpawnRate;
		sr = enemyPrefab.GetComponent<SpriteRenderer> ();
	}
	
	// Update is called once per frame
	void Update () {

		if (deltaSpawn > 0) {
			deltaSpawn -= Time.deltaTime;
		} else {

			// Calculating camera boundaries
			//var dist = (transform.position - Camera.main.transform.position).z;

			var rightBorder = Camera.main.ViewportToWorldPoint(
				new Vector3(1, 0, 0)
			).x;

			var topBorder = Camera.main.ViewportToWorldPoint(
				new Vector3(0, 0, 0)
			).y;

			var bottomBorder = Camera.main.ViewportToWorldPoint(
				new Vector3(0, 1, 0)
			).y;

			Vector3 position = new Vector3(rightBorder,Random.Range(bottomBorder-sr.bounds.size.y/2,topBorder+sr.bounds.size.y/2),0.0f);
			Instantiate (enemyPrefab,position,enemyPrefab.transform.rotation);
			deltaSpawn = SpawnRate;
		}
	}
}