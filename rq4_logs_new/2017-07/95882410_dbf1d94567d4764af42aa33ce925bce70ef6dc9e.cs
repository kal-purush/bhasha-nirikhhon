using UnityEngine;
using System.Collections;

public class Bomb : MonoBehaviour {

	//bomb skin
	public Sprite skin;
	//bomb explosion radius
	public float explosionRadius;
	//power of the throw
	public Vector3 throwPower;
	//bomb damage
	public float damage;
	//birth time of the bomb
	private float birthTime;
	//life time
	public float lifeTime = 10f;
	//list of all damagable objects
	public IDestroyableObject destroyableList;
	//player
	public bool playerFacingRight;

	private Rigidbody2D rb2D;

	void Start () {
		birthTime = Time.time;
		rb2D = GetComponent<Rigidbody2D> ();
		playerFacingRight = GameObject.FindGameObjectWithTag("Player").GetComponent<Player>().facingRight;
		if (playerFacingRight)
			rb2D.AddForce (new Vector3(200f,200f,0));
		else
			rb2D.AddForce (new Vector3(-200f,200f,0));
	}

	void FixedUpdate () {
		if (Time.time - birthTime > lifeTime) {
			Explode ();
		}

		rb2D.AddTorque (2f);
	}

	void OnCollisionEnter2D (Collision2D col) {
		if ((col.gameObject.tag == "Wall")||(col.gameObject.tag == "Enemy") || (col.gameObject.tag == "Ground")) {
			Explode ();
		}
	}

	void Explode () {
		//explosion animation
				
		/*
		destroyableList = GameObject.FindObjectsOfType<IDestroyableObject> ();
		foreach (IDestroyableObject dobj in destroyableList) {
			dobj.GetDamage (damage);
		}
		*/

		this.enabled = false;
		Destroy (this.gameObject);
	}
}