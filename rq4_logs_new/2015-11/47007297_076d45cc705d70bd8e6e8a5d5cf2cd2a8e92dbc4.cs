using UnityEngine;
using System.Collections;

public class BulletSc: MonoBehaviour {
	public GameObject bullet;
	public Transform Spawn;
	public float speed = 1000.0f;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKeyDown (KeyCode.Z)) {
			Shot ();
		}

	
	}
	void Shot(){
		GameObject obj = GameObject.Instantiate (bullet)as GameObject;
		obj.transform.position = Spawn.transform.position;
		Vector3 force;
		force= this.gameObject.transform.forward * speed;

		obj.GetComponent<Rigidbody> ().AddForce (force);

	
	}
}