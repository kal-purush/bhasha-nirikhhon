/*using UnityEngine;
using System.Collections;

public class Controller : MonoBehaviour {

	Rigidbody rb;
	public float accel= 10;
	// Use this for initialization
	void Start () {
		rb = this.GetComponent<Rigidbody> ();
	}
	
	// Update is called once per frame
	void FixedUpdate () {

		Vector3 relativePos = Input.mousePosition - transform.position;
		Quaternion rotation = Quaternion.LookRotation(relativePos);
		transform.rotation = rotation;

			float DeplacementHorizontal = Input.GetAxis ("Horizontal");
			float DeplacementVertical = Input.GetAxis ("Vertical");

		Vector3 deplacement = new Vector3 (DeplacementHorizontal, 0, DeplacementVertical);
		deplacement = transform.TransformDirection (deplacement);
		rb.velocity = deplacement * accel;
	}
}*/



using UnityEngine;
using System.Collections;


public class Controller : MonoBehaviour {
	
	
	public float speed = 90.0f;		// max speed of camera
	public float sensitivity = 0.25f; 		// keep it from 0..1
	public bool inverted = false;
	
	
	private Vector3 lastMouse = new Vector3(255, 255, 255);
	
	
	// smoothing
	public bool smooth = true;
	public float acceleration = 0.1f;
	private float actSpeed = 0.0f;			// keep it from 0 to 1
	private Vector3 lastDir = new Vector3();
	
	
	// Use this for initialization
	void Start () {
		
		
	}
	
	
	
	
	
	
	// Update is called once per frame
	void Update()
	{
		
		// Mouse Look
		lastMouse = Input.mousePosition - lastMouse;
		if (!inverted) lastMouse.y = -lastMouse.y;
		lastMouse *= sensitivity;
		lastMouse = new Vector3(transform.eulerAngles.x + lastMouse.y, transform.eulerAngles.y + lastMouse.x, 0);
		transform.eulerAngles = lastMouse;
		lastMouse = Input.mousePosition;
		
		//Mouvement du vaisseau
		//transform.Rotate(Input.mousePosition, 10f);
		//transform.Rotate(Vector3.down, Time.deltaTime, Space.World);
		
		// Movement of the camera
		Vector3 dir = new Vector3();			// create (0,0,0)
		
		if (Input.GetKey(KeyCode.Z)) dir.z += 5.0f;
		if (Input.GetKey(KeyCode.S)) dir.z -= 5.0f;
		if (Input.GetKey(KeyCode.Q)) dir.x -= 5.0f;
		if (Input.GetKey(KeyCode.D)) dir.x += 5.0f;
		dir.Normalize();
		
		
		if (dir != Vector3.zero)
		{
			// some movement 
			if (actSpeed < 5)
				actSpeed += acceleration * Time.deltaTime * 40;
			else
				actSpeed = 5.0f;
			
			lastDir = dir;
		}
		else
		{
			// should stop
			if (actSpeed > 0)
				actSpeed -= acceleration * Time.deltaTime * 20;
			else
				actSpeed = 0.0f;
		}
		
		
		
		//import
		if (smooth)
			transform.Translate(lastDir * actSpeed * speed * Time.deltaTime);
		
		else
			transform.Translate(dir * speed * Time.deltaTime);
		
		
		var rotateSpeed = 150.0;
		
		var rotation = Input.GetAxis("Horizontal") * rotateSpeed;
		
		var vTranslation = Input.GetAxis("Vertical") * actSpeed;
		var hTranslation = Input.GetAxis("Horizontal") * actSpeed;
		
		
		vTranslation *= Time.deltaTime;
		hTranslation *= Time.deltaTime;
		rotation *= Time.deltaTime;
		
		
		
		
		// Allow turning at anytime. Keep the character facing in the same direction as the Camera if the right mouse button is down.
		if (Input.GetMouseButton(1))
		{
			transform.rotation = Quaternion.Euler(Camera.current.transform.eulerAngles.x, Camera.current.transform.eulerAngles.y, 0);
			
		}
		
		/*
        Move our player along it's new vertical axis
        */
		
		transform.Translate(0, 0, vTranslation);
		transform.Rotate(0, (float)rotation, 0);
		
		
		/*
        This locks the cursor - making it invisible to the user.
        */
		
		//if (Input.GetMouseButton(1) || Input.GetMouseButton(0))
		//    Screen.lockCursor = true;
		//else
		//    Screen.lockCursor = false;
		
		
		
		
	}
	
	void OnGUI() {
		GUILayout.Box ("actSpeed: " + actSpeed.ToString());
	}
}

		                               