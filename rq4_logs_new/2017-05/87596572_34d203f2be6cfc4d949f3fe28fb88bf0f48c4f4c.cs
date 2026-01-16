using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class player : MonoBehaviour
{

	CharacterController control;

	[Range (40f, 100f)]
	public float rotationVal;
	public float playerSpeed, sprintSpeed, crouchSpeed;
	float horizontal, vertical, upDownLook, leftRightLook;
	public bool isCrouching = false;


	RaycastHit hit;
	bool carryingObject = false;
	public string HeldObjectName = "";
	int layerMask = 1 << 8;

	// Use this for initialization
	void Start ()
	{

		control = GetComponent<CharacterController> ();

	}
	
	// Update is called once per frame
	void Update ()
	{
		Cursor.lockState = CursorLockMode.Locked;

		horizontal = Input.GetAxis ("Horizontal");
		vertical = Input.GetAxis ("Vertical");
		upDownLook -= Input.GetAxis ("Mouse Y") * Time.deltaTime * rotationVal;
		upDownLook = Mathf.Clamp (upDownLook, -80f, 80f);
		leftRightLook = Input.GetAxis ("Mouse X") * Time.deltaTime * rotationVal;


		transform.Rotate (0, leftRightLook, 0);
		Camera.main.transform.eulerAngles = new Vector3 (upDownLook, transform.eulerAngles.y, 0);

		control.Move (transform.up * -Time.deltaTime * 10f);//gravity

		//movement stuff
		if (Input.GetKey (KeyCode.LeftShift)) {//if sprinting
			control.Move (transform.forward * Time.deltaTime * vertical * sprintSpeed);
			control.Move (transform.right * Time.deltaTime * horizontal * sprintSpeed);
		} else {
			if (isCrouching) {//if crouching
				control.Move (transform.forward * Time.deltaTime * vertical * crouchSpeed);
				control.Move (transform.right * Time.deltaTime * horizontal * crouchSpeed);
			} else {//normal
				control.Move (transform.forward * Time.deltaTime * vertical * playerSpeed);
				control.Move (transform.right * Time.deltaTime * horizontal * playerSpeed);
			}
		}

		//jump
		if (Input.GetKey (KeyCode.Space) && control.isGrounded) {
			control.Move (transform.up * Time.deltaTime * 100f);
		}

		//crouch stuff
		if (Input.GetKeyDown (KeyCode.C) && !isCrouching) {

			isCrouching = true;
		} else if (Input.GetKeyDown (KeyCode.C) && isCrouching) {

			isCrouching = false;
		}
		if (isCrouching) {
			control.height = 1;
			Camera.main.transform.localPosition = Vector3.zero;
		} else {
			control.height = 2;
			Camera.main.transform.localPosition = new Vector3 (0, 0.75f, 0);
		}


		InteractionFunction ();


		Debug.DrawRay(Camera.main.transform.position, Camera.main.transform.forward,Color.red);
		
	}

	void InteractionFunction ()
	{
		//this creates raycast in front of player used to grab objects
		Ray playerRay = new Ray (Camera.main.transform.position, Camera.main.transform.forward);

		//this is so the raycast only hits stuff on the 'pickable' layer 
		if (Physics.Raycast (playerRay, out hit,3,layerMask)) {


			if (hit.collider != null) {
				GameObject col = hit.collider.gameObject;
				Debug.Log (col.layer);

				//Grab object if mouse clicked (also freezes object at center of screen and slightly moves the player's collision box so object doesn't go through walls)
				if (Input.GetMouseButtonDown (0) && !carryingObject) {
					if (!carryingObject) {
						HeldObjectName = col.name;
							
						col.transform.parent = this.transform;
						col.transform.localPosition = Vector3.forward *1.2f;
						col.transform.localEulerAngles = Vector3.zero;
						control.center = new Vector3 (0, 0, 0.5f);
						carryingObject = true;
						hit.rigidbody.constraints = RigidbodyConstraints.FreezeAll;
					}
				}
				else if (Input.GetMouseButtonDown (0) && carryingObject) {

					this.transform.FindChild (HeldObjectName).GetComponent<Rigidbody> ().constraints = RigidbodyConstraints.None;
					this.transform.FindChild (HeldObjectName).parent = null;
					control.center = new Vector3 (0, 0, 0);
					carryingObject = false;
					HeldObjectName = "";
				}
			}
		}
	}
}