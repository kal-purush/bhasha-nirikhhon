using UnityEngine;
using System.Collections;

public class Bullet : MonoBehaviour {

	//bullet moving speed
	public float bulletSpeed;

	//bullet damage
	public float damage;

	//timebirth
	float birthTime;
	//lifrtime of bullet
	public float lifetime = 10;

	//move bullet formard
	void Move(){
		this.transform.Translate (new Vector3(bulletSpeed*Time.deltaTime, 0, 0));	
	}


	// Use this for initialization
	void Start () {
		birthTime = Time.time;
	}
	
	// Update is called once per frame
	void Update () {
		Move ();
		//preventing bullet from stucking
		if (Time.time - birthTime > lifetime) {
			Destroy (this.gameObject);
		}
	}

	void OnCollisionEnter2D  (Collision2D coll){
		/*foreach(GameObject item in collision.gameObject){
			Debug.Log("bam!");
		}*/
		Debug.Log ("!!!!");
		Debug.Log (coll.gameObject.tag);
		if ((coll.gameObject.tag == "Wall")||(coll.gameObject.tag == "Enemy")) {
			Destroy (this.gameObject);
		}
			
	}
	
}