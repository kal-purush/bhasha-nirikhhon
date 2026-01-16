using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Tower : MonoBehaviour {

	public TowerUnit minionPrefab;

	public float spawnCooldown;

	public int maxMinionsFromTower;

	private List<TowerUnit> livingMinions;

	private float lastSpawnTime;

	[HideInInspector]
	public Markup TowerMarkup;

	[HideInInspector]
	public Markup MinionsMarkup;



	// Use this for initialization
	void Start () {
		livingMinions = new List<TowerUnit> ();


		for (int a = 0; a < maxMinionsFromTower; a++) {
			SpawnMinion();
		}

		lastSpawnTime = Time.time;
	}
	
	// Update is called once per frame
	void Update () {
		if (livingMinions.Count < maxMinionsFromTower && lastSpawnTime + spawnCooldown <= Time.time) {
			SpawnMinion();
		}
	}

	public void MinionDestroyed(TowerUnit minion){
		if(livingMinions.Contains(minion))
			livingMinions.Remove (minion);
	}

	private TowerUnit SpawnMinion(){
		TowerUnit minion = (TowerUnit) Instantiate(minionPrefab, this.transform.position, Quaternion.identity);

		livingMinions.Add (minion);

		Markup available = null;
		for (int a = 0; a < MinionsMarkup.transform.childCount && available == null; a++) {
			if(MinionsMarkup.transform.GetChild(a).GetComponent<Markup>().available)
				available = MinionsMarkup.transform.GetChild(a).GetComponent<Markup>();
		}

		minion.destinationPosition = available;
		available.available = false;

		minion.spawner = this;

		return minion;
	}
}