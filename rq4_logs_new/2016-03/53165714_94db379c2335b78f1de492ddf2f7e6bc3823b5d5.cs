using UnityEngine;
using System.Collections;

public class movement : MonoBehaviour {
	public Transform snowman;
	private float rotationRate = 3f;



	void Awake(){
		snowman = GetComponent<Transform> ();

	}

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		float rotation = Input.GetAxis ("Horizontal") * rotationRate;
		snowman.Rotate (0, rotation, 0);
//		if (Input.GetKeyDown(KeyCode.RightArrow)){
//			
//			snowman.rotation = Quaternion ();
//				}
	}
}