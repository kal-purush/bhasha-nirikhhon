using UnityEngine;
using System.Collections;

public class player_move : MonoBehaviour {

	public float hitrost = 4f;
	public float skok = 0.1f;

	


	void skoci(){
		if(Input.GetKey(KeyCode.Space)){
			GetComponent<Rigidbody2D>().AddForce(new Vector2(0, 1), ForceMode2D.Impulse);
		}
	}

	void levo(){
		GetComponent<Rigidbody2D>().velocity = (Vector3.left * 100 * 2 * Time.deltaTime);
	}


	void desno(){
		GetComponent<Rigidbody2D>().velocity = (Vector3.right * 100 * 2 * Time.deltaTime);
	}
	
	// Use this for initialization
	void Start () {
	
	}


	// Update is called once per frame
	void Update () {


		// levo

		if(Input.GetKey(KeyCode.LeftArrow)){
			levo ();
		}

		// desno

		if (Input.GetKey (KeyCode.RightArrow)) {
			desno ();
		}

		if (Input.GetKey("space")) {
				
			GetComponent<Rigidbody2D>().AddForce(new Vector2(0,skok), ForceMode2D.Impulse);

		}


	}

}