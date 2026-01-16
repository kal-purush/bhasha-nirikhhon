using UnityEngine;
using System.Collections;

public class Enemy : MonoBehaviour {

	//speed of enemy movement
	public float enemySpeed;

	//enemy max hp
	public float enemyHp;

	//enemy damage
	public float enemyDamage;

	//movement vector
	public Vector3 moveVector = new Vector3 ( 1, 0, 0);

	BoxCollider2D thisCollider;

	// Use this for initialization
	void Start () {
		thisCollider = this.GetComponent<BoxCollider2D> ();
	}
	
	// Update is called once per frame
	void Update () {
	}

	void FixedUpdate(){
		Move ();
	}

	void Move(){
		this.transform.Translate (moveVector * enemySpeed * Time.deltaTime);
	}

	void getDamage (float damage){
		enemyHp -= damage;
		if (enemyHp < 0) {
			Destroy (this.gameObject);
		}
	}

	void OnCollisionEnter2D(Collision2D coll){
		if (coll.collider.tag == "Bullet"){
			getDamage(coll.collider.gameObject.GetComponent<Bullet> ().damage);
		}

		//reverse scale if collidessmth exept player
		if ((coll.collider.tag != "Player") && (coll.collider.tag != "Bullet")) {

			RaycastHit2D[] hits;
			thisCollider.enabled = false;
			Vector3 end = this.transform.position;

			end.x = end.x + thisCollider.size.x / 2 * this.transform.localScale.x;

			end += moveVector * enemySpeed * Time.deltaTime;
			hits = Physics2D.LinecastAll (this.transform.position, end);

			Debug.DrawLine (this.transform.position, end, Color.red);
			thisCollider.enabled = true;

			if (hits.Length != 0) {
				foreach (var hit in hits) {
					if ((hit.collider.tag != "Player") && (hit.collider.tag != "Bullet")) {
						Debug.Log ("TURN");
						this.transform.localScale = new Vector3 (this.transform.lossyScale.x * -1, this.transform.lossyScale.y, this.transform.lossyScale.z);
						moveVector *= -1;
					}
				}
			}
		}
	
			

	}

}