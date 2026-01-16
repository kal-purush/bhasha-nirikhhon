using UnityEngine;
using System.Collections;

public class EnemySpawner : MonoBehaviour {
	
	public GameObject enemy;
	// Use this for initialization
	void Start () 
	{
		InvokeRepeating ("SpawnEnemy", 0f, 2f);
	}
	
	// Update is called once per frame
	void Update () 
	{
		
	}
	
	void SpawnEnemy ()
	{
		var _enemy = Instantiate(enemy, transform.position + new Vector3(0, Random.Range (-1, -10), 0), Quaternion.identity) as GameObject;
		int rand = Random.Range (0, 2);
		
		if (rand == 0)
			_enemy.transform.eulerAngles = new Vector3 (0, 0, 90);
		
	}
}