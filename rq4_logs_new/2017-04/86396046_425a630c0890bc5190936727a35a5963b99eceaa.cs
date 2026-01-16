using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WaveManager : MonoBehaviour {

	public List<GameObject> 	Waves;						// GameObject must be an empty GameObject with BasicEnemies as children
	public bool 				RandomWave;
	public int 					EnemyMax;					// Maximum number of enemies display

	List< List<BasicEnnemy> >	WaveLeft;	
	List<BasicEnnemy>			EnemiesForCurrentWave;

	float 						TimeLeft;					// Min time between each enemy summon
	float						NbrEnemiesOnScreen = 0;		// Actual number of enemies 

	List<Vector2>				SpawnerPositionList;

	// Update is called once per frame
	void Update () {}

	// Use this for initialization
	void Start ()
	{
		// Generate random waves
		if (RandomWave)
			GenerateRandomWave (); // Not implement yet

		// Use waves 
		else
		{
			// SET WaveLeft
			foreach (GameObject waveObj in Waves) 
			{
				BasicEnnemy[] wave = waveObj.GetComponentsInChildren<BasicEnnemy> ();

				List<BasicEnnemy> tmpEnemyList = new List<BasicEnnemy>();
				foreach (BasicEnnemy enemy in wave)
					tmpEnemyList.Add (enemy);

				WaveLeft.Add (tmpEnemyList);
			}

			// SET EnemiesForCurrentWave
			EnemiesForCurrentWave = WaveLeft [0];
			WaveLeft.RemoveAt (0);
		}

		// SET TimeLeft
		if (EnemiesForCurrentWave.Count >= 2)
			TimeLeft = EnemiesForCurrentWave [1].SpawnCooldown;

		// Set Spawner position
		SpawnerPositionList = new List<Vector2>();
		SpawnerPositionList.Add (new Vector2 (0, 0));

	}

	void Summon()
	{
		// New wave
		if (EnemiesForCurrentWave.Count <= 0)
		{
			if (WaveLeft.Count > 0)
			{
				EnemiesForCurrentWave = WaveLeft [0];
				WaveLeft.RemoveAt (0);
			}
		}

		// The wave continue
		if (EnemiesForCurrentWave.Count >= 1)
		{
			Transform BasicEnemy = EnemiesForCurrentWave [0].transform;
			int randomIndex = Random.Range(0, SpawnerPositionList.Count - 1);

			Instantiate(BasicEnemy, SpawnerPositionList[randomIndex], Quaternion.identity);
			NbrEnemiesOnScreen++;

			if (EnemiesForCurrentWave.Count >= 2)
				TimeLeft = EnemiesForCurrentWave [1].SpawnCooldown;

			EnemiesForCurrentWave.RemoveAt(0);
		}
	}

	IEnumerator SummonLater(float delay)
	{
		float TimeUntilSummon = delay;

		NbrEnemiesOnScreen++;

		while (TimeUntilSummon > 0)
		{
			TimeUntilSummon -= Time.deltaTime;
			yield return null;
		}

		NbrEnemiesOnScreen--;

		Summon ();
	}

	void GenerateRandomWave()
	{
		// Implement it later
	}

	// The GameManager must call this function each time an enemy die
	public void EnemyDie(GameObject enemyObj)
	{
		// Use enemyObj for BossPart enemy

		NbrEnemiesOnScreen--;
	}

	// The GameManager must start this coroutine
	IEnumerator StartWaveManager()
	{
		while (Waves.Count > 0)
		{
			if (TimeLeft >= 0)
			{
				TimeLeft -= Time.deltaTime;
			}

			if (NbrEnemiesOnScreen < EnemyMax)
			{
				if (TimeLeft == 0) {
					Summon ();
				}

				else {
					StartCoroutine ("SummonLater", TimeLeft);
				}
			}

			yield return null;
		}
	}
}