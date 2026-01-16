using UnityEngine;
using System.Collections;

public class Player : MonoBehaviour {

	public float speed = 10f;
	public float jumpHeight = 1f;
	public Sprite skin;
	public float currentHp;
	public float maximumHp;
	public int bombMaxCount;
	public float bombAccuracy;
	public int bombsPerThrow;
	public Vector3 bombThrowVector;

	public float jumpForce = 1000f;
	public Transform groundCheck;
	public float gravityScale = 7f;
	public float invulnerabilityTime = 2f;

	private bool invulnerable = false;
	private bool grounded = true;
	private bool isJumping = false;
	private bool facingRight = false;
	private Vector3 direction;
	private Rigidbody2D rb2D;


	void Start () {
		rb2D = GetComponent<Rigidbody2D> ();
		rb2D.gravityScale = gravityScale;
	}

	void Update () {
		CheckMove ();
		CheckJump ();
	}

	void FixedUpdate () {
		Move ();
		Jump ();
	}

	void CheckMove () {
		direction = new Vector3 (0, 0, 0);

		if (Input.GetKey (KeyCode.D) || Input.GetKey (KeyCode.RightArrow)) {
			direction += Vector3.right;
			facingRight = true;
		}

		if (Input.GetKey (KeyCode.A) || Input.GetKey (KeyCode.LeftArrow)) {
			direction += Vector3.left;
			facingRight = false;
		}
	}

	void CheckJump () {
		grounded = Physics2D.Linecast (transform.position, groundCheck.position, 1 << LayerMask.NameToLayer ("Ground"));

		if ((Input.GetKey (KeyCode.Space) || Input.GetKey (KeyCode.W))  && grounded) {
			isJumping = true;
		}
	}

	void Move () {
		if (direction != new Vector3 (0, 0, 0))
			transform.Translate (speed * direction * Time.deltaTime);
	}
		
	void Jump () {
		if (isJumping) {
			rb2D.AddForce (new Vector2 (0f, jumpForce * jumpHeight));
			isJumping = false;
		}
	}

	void Bomb () {
		Debug.Log ("Bomb has been planted."); 
	}

	void OnCollisionEnter2D (Collider2D col) {
		if (col.tag == "Enemy") {

		}
	}

	void Invulnerability () {
		if (invulnerable) {

		}
	}
}