using UnityEngine;
using System.Collections;

public class AI1 : MonoBehaviour {

	private Vector3 Player;
	private Vector2 Playerdirection;
	private float Xdiff;
	private float Ydiff;

	public float speed;

	private Rigidbody2D myRigidbody;

	void Start () 
	{
		myRigidbody = GetComponent<Rigidbody2D> ();
		//speed = 2;
	}
	

	void Update () 
	{
		Player = GameObject.Find ("Player").transform.position;

		Xdiff = Player.x - transform.position.x;
		Ydiff = Player.y - transform.position.y;

		Playerdirection = new Vector2 (Xdiff, Ydiff);

		myRigidbody.AddForce (Playerdirection.normalized * speed);
	}
}