using UnityEngine;
using System.Collections;

public class SideEnemy :BaseEnemy {

	private bool toRight = false; 
	// Use this for initialization
	public void Start () {
		base.Start ();
		toRight = (Random.Range (0, 2) >= 1);
		if (toRight) {
			transform.position = new Vector2 (transform.position.x, Random.Range (-0.3f, 0.5f));
		} else {
			transform.position = new Vector2 (-transform.position.x, Random.Range (-0.3f, 0.5f));
		}
	}
	
	// Update is called once per frame
	public void Update () {
		base.Update ();
		if (toRight) {
			base.r2d.velocity = new Vector2 (3, -1);
		} else {
			base.r2d.velocity = new Vector2 (-3, -1);
		}
	}
}