using UnityEngine;
using System.Collections;

public class AI : MonoBehaviour {
	public Rigidbody rbody; 
	// Use this for initialization
	void Start () {
		rbody = GetComponent<Rigidbody> (); 
	}
	
	// Update is called once per frame
	void Update () {
		Ray ray = new Ray (transform.position, transform.right); 
		RaycastHit rayInfo = new RaycastHit();
		if (Physics.Raycast (ray, out rayInfo, 5f)){
			Debug.DrawRay(ray.origin, ray.direction * 10, Color.yellow);
			if (rayInfo.collider.tag == "obstacle"){
				rbody.velocity = new Vector3(0f, 10f, 0f); 

			}
		}
	
	}
}