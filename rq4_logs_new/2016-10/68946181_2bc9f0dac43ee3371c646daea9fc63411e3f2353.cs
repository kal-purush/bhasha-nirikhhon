using UnityEngine;

public class EnemyCreator : MonoBehaviour
{
    public GameObject enemy;                // The enemy prefab to be spawned.
	public Transform[] spawnPoints;         // An array of the spawn points this enemy can spawn from.
	
	void Start()
	{
		// Create an instance of the enemy prefab at the randomly selected spawn point's position and rotation.
		for(int i = 0; i < spawnPoints.Length; i++)
		{
			Instantiate (enemy, spawnPoints[i].position, spawnPoints[i].rotation);
		}
	}
}