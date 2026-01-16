using UnityEngine;
using System.Collections;
using UnityEngine.Networking;


public class PlayerController : NetworkBehaviour 
{	
	
	private float movementspeed = 5.0f;
	private float turnSpeed = 45.0f;

	private float cameraHeight = 4f;
	private float cameraDistance = 6f;

	private Rigidbody rigidbody;
	private Transform mainCamera;
	private Vector3 cameraOffset;

	void Start()
	{
		if (!isLocalPlayer)
		{
			Destroy (this);
			return;
		}

		rigidbody = GetComponent<Rigidbody> ();
		cameraOffset = new Vector3 (0f, cameraHeight, -cameraDistance);
		mainCamera = Camera.main.transform;
		MoveCamera ();


	}
	
	void Update ()
	{
		float moveHorizontal = Input.GetAxis("Horizontal");
		float moveVertical = Input.GetAxis("Vertical");
		
		Vector3 deltaTranslation = transform.position + transform.forward*movementspeed*moveVertical*Time.deltaTime;
		rigidbody.MovePosition (deltaTranslation);

		Quaternion deltaRotation = Quaternion.Euler (turnSpeed * new Vector3 (0, moveHorizontal, 0) * Time.deltaTime);
		rigidbody.MoveRotation (rigidbody.rotation * deltaRotation);

		MoveCamera ();

	}
	
//	void OnTriggerEnter(Collider other) 
//	{
//		if(other.gameObject.tag == "PickUp")
//		{
//			GameObject obj = other.gameObject as GameObject;
//			Cmd_DestroyThis(obj);

//		}
//	}
		
	//[Command]
//	void Cmd_DestroyThis(GameObject obj)
//	{
//		NetworkServer.Destroy(obj);
//	}

	void MoveCamera(){
		mainCamera.position = transform.position;
		mainCamera.rotation = transform.rotation;
		mainCamera.Translate (cameraOffset);
		mainCamera.LookAt (transform);

	}


}