using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Networking;

public class EnemySpawn : NetworkBehaviour {
	[SerializeField] private GameObject objectToSpawn;
	[SerializeField] private Transform spawnPoint;

	// Use this for initialization
	void Start() {

	}

	// Update is called once per frame
	private void Update() {
		if (!isLocalPlayer) { return; }

		if (Input.GetMouseButtonDown(1)) {
			CmdSpawn();
		}
	}

	[Command]
	private void CmdSpawn() {
		GameObject go = Instantiate(objectToSpawn, spawnPoint);
		NetworkServer.Spawn(go);
	}
}