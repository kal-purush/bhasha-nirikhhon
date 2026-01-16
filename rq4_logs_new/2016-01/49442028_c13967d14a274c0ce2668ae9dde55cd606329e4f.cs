using UnityEngine;
using System.Collections;

public class Jump : MonoBehaviour {
	public float jumpSpeed = 0f;
	private float jumpStrength = 10f;
	private bool jumping = false;
	Rigidbody rbody; 
	// Use this for initialization
	void Start () {
		rbody = GetComponent<Rigidbody> (); 
	}
	//x+=(target-x)*0.5f

	// Update is called once per frame
	void Update () {
		if (Input.GetKey (KeyCode.Space) && !jumping) {
			jumpStrength += (jumpSpeed - jumpStrength)*0.1f;
			Debug.Log ("The jump strength is " + jumpStrength); 
		}
		if (Input.GetKeyUp (KeyCode.Space) && !jumping) {
			Debug.Log ("Penis" + jumpStrength); 
			rbody.velocity = (new Vector3( 0f, jumpStrength, 0f));
			jumpStrength=0f;
			jumping=true;
			Debug.Log (jumpStrength); 

		}
		if (rbody.velocity.y == 0) {
			jumping = false;
		}
	}
}