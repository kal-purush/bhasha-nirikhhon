using UnityEngine;
using System.Collections;

public class ProjectileMovement : MonoBehaviour {

	public enum BulletColor{ Red, Green, Blue };

	public BulletColor color = BulletColor.Red;
	public Vector2 direction = new Vector2();
	public float speed = 30f;
	public float spread = 1f;

	private Rigidbody2D rigidbody2D;


	// Use this for initialization
	void Start () {
		rigidbody2D = gameObject.GetComponent<Rigidbody2D>();
	}
	
	// Update is called once per frame
	void Update () {
		
	}

	void FixedUpdate () {
		rigidbody2D.velocity = direction.normalized * speed;
	}
}