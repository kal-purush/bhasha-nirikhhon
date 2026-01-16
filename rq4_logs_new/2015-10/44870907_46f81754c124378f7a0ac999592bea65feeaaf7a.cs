using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class MissileCommandManager : MonoBehaviour {

	public Transform missileTarget;
	public float timeBetweenLaunches;
	public int maxMissilesMidair;
	public List<Transform> spawns;
	public GameObject missilePrefab;
	public List<GameObject> midairMissiles;
	public static MissileCommandManager instance;

	public void OnMissileHit( Collision col ) {
		if( col.gameObject.CompareTag( "Player" ) ) {
			GameManager.instance.OnMissileHit();
		}
		midairMissiles.Remove( col.gameObject );
	}

	public void LaunchMissile() {
		Debug.Log( "Launching Missile" );
		int spawner = Random.Range( 0, spawns.Count );
		midairMissiles.Add( Instantiate( missilePrefab, spawns[spawner].position, spawns[spawner].rotation ) as GameObject );
	}

	void Awake() {
		if( instance == null ) {
			instance = this;
		}
	}

	// Use this for initialization
	void Start () {
		StartLaunchLoop();
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	public void StartLaunchLoop() {
		StartCoroutine( LaunchLoop() );

	}

	IEnumerator LaunchLoop() {
		yield return new WaitForSeconds( timeBetweenLaunches );
		bool shot = false;
		while( !shot ) {
			if( midairMissiles.Count < maxMissilesMidair ) {
				LaunchMissile();
				shot = true;
			}
			yield return 0;
		}
		
		
	}
}