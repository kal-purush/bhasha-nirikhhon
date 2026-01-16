using UnityEngine;
using System.Collections;

public class EnemyPathController : MonoBehaviour {

	// Use this for initialization
	void Start () {
		GameObjectsManager.enemyPath = this.gameObject;
	}
}