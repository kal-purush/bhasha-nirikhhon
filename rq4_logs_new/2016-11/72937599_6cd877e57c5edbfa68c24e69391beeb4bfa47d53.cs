using UnityEngine;
using System.Collections;

public class ShotBehaviourHorizontal : MonoBehaviour {

	public float speed;
	public GameObject shooter;

	private bool flipped = false;

	Rigidbody2D rb;

	void Start ()
	{ 
		rb = GetComponent<Rigidbody2D> ();

		if (shooter.transform.localScale.y < 0f)
			flipped = true;
		else
			flipped = false;
	}

	void OnEnable()
	{
		if (shooter.transform.localScale.y < 0f)
			flipped = true;
		else
			flipped = false;
	}

	void Update () {

		if (flipped)
			rb.velocity = new Vector2 (speed, 0f);
		else
			rb.velocity = new Vector2 (-speed, 0f);
	}

	void OnCollisionEnter2D(Collision2D coll) {

		if (coll.gameObject.tag != "Shot")
			gameObject.SetActive (false);
		if (coll.gameObject.tag == "Shot")
			flipped = !flipped;

	}
}