using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraControlByMouse : MonoBehaviour {

	public Plane plane;
	public Camera cam;
	public float RotateSpeed = 2;
	public float ZoomSpeed = 0.125f;
	public float smoothspeed = 0.125f;

	Vector3 prevMousePos;
	Vector2 MousePosDiff = Vector2.zero;
	Vector2 PointDiff = Vector2.zero;

	// Use this for initialization
	void Start () {
		cam = GetComponent<Camera> ();
		plane = GetComponent<Plane> ();
	}

	// After Update
	void FixedUpdate(){
		Vector3 CurMousePos = Input.mousePosition;
		MousePosDiff = new Vector2 (CurMousePos.x - prevMousePos.x, CurMousePos.y - prevMousePos.y);

		Ray prevRay = cam.ScreenPointToRay (prevMousePos);
		RaycastHit prevHit;
		Physics.Raycast(prevRay, out prevHit);

		Ray curRay = cam.ScreenPointToRay (CurMousePos);
		RaycastHit curHit;
		Physics.Raycast(curRay, out curHit);

		PointDiff = new Vector2 (curHit.point.x - prevHit.point.x, curHit.point.z - prevHit.point.z);
		prevMousePos = CurMousePos;
	}

	// Update is called once per frame
	void Update () {
		// Rotate left/right
		if (Input.GetButton ("Fire1") && Input.GetButton ("Fire2") && MousePosDiff != Vector2.zero) {
			Ray ray = cam.ScreenPointToRay (new Vector3 (Screen.width / 2, Screen.height / 2));
			RaycastHit hit;
			Physics.Raycast (ray, out hit);

			Debug.Log (hit.point);
			Vector3 axis = new Vector3 (0, (MousePosDiff.x + MousePosDiff.y) / 2, 0);
			transform.RotateAround (hit.point, axis, RotateSpeed);
		} else {
			// Move camera with y = const
			if (Input.GetButton ("Fire1") && MousePosDiff != Vector2.zero) {
				
				Vector3 desiredPos = new Vector3 (transform.position.x - PointDiff.x/smoothspeed, transform.position.y, transform.position.z - PointDiff.y/smoothspeed);
				transform.position = Vector3.Lerp (transform.position, desiredPos, smoothspeed);
			}

			Debug.Log (transform.eulerAngles);
			// Rotate up/down
			if (Input.GetButton ("Fire2") && MousePosDiff != Vector2.zero) {

				//float value  = Mathf.Min(Mathf.Abs (MousePosDiff.y), 20.0f) * Mathf.Abs (MousePosDiff.y) / MousePosDiff.y;
				Vector3 rot = new Vector3 (MousePosDiff.y, 0, 0);
				Debug.Log (rot);
				if (transform.eulerAngles.x >= 15.0f && transform.eulerAngles.x <= 80.0f) 
					transform.Rotate (rot * RotateSpeed / 10);
				
				if (transform.eulerAngles.x < 15.0f)
					transform.Rotate (-rot * RotateSpeed / 10);

				if (transform.eulerAngles.x > 80.0f)
					transform.Rotate (-rot * RotateSpeed / 10);
			}
		}
			
		// Zoom
		if (Input.mouseScrollDelta.y != 0) {
			Ray ray = cam.ScreenPointToRay (Input.mousePosition);
			RaycastHit hit;
			Physics.Raycast (ray, out hit);

			if (Input.mouseScrollDelta.y > 0) transform.position = Vector3.Lerp(transform.position, hit.point, ZoomSpeed);
			else transform.position = Vector3.Lerp(transform.position, 2*transform.position - hit.point, ZoomSpeed);
		}

	}
}