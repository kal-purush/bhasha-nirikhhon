using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CamController : MonoBehaviour {
	public Text txt;
	public Camera cam;
	public float RotateSpeed;
	public float ZoomSpeed;
	public float orthoZoomSpeed;
	public float perspectiveZoomSpeed;
	// Use this for initialization
	void Start () {
		cam = GetComponent<Camera> ();
	}

	void Focus(Vector3 Position){
	
	
	
	}

	void Update()
	{
		if (Input.touchCount == 1 && Input.GetTouch(0).phase == TouchPhase.Moved)
		{
			// Get movement of the finger since last frame
			Vector2 touchDeltaPosition = Input.GetTouch(0).deltaPosition;
			if (Mathf.Abs(transform.rotation.x) >= 85.0f) {
				touchDeltaPosition.y = 0.0f;
			}
			Vector3 rot = new Vector3 (touchDeltaPosition.y, touchDeltaPosition.x, 0.0f);
			transform.Rotate (rot * RotateSpeed);
			txt.text += touchDeltaPosition.y.ToString () + (-touchDeltaPosition.x).ToString ();
		}
		if (Input.touchCount == 0) {
			transform.Rotate (Vector3.zero);
		}
		// If there are two touches on the device...
		if (Input.touchCount == 2)
		{
			// Store both touches.
			Touch touchZero = Input.GetTouch(0);
			Touch touchOne = Input.GetTouch(1);

			// Find the position in the previous frame of each touch.
			Vector2 touchZeroPrevPos = touchZero.position - touchZero.deltaPosition;
			Vector2 touchOnePrevPos = touchOne.position - touchOne.deltaPosition;

			// Find the magnitude of the vector (the distance) between the touches in each frame.
			float prevTouchDeltaMag = (touchZeroPrevPos - touchOnePrevPos).magnitude;
			float touchDeltaMag = (touchZero.position - touchOne.position).magnitude;

			// Find the difference in the distances between each frame.
			float deltaMagnitudeDiff = prevTouchDeltaMag - touchDeltaMag;
			Vector3 pos = transform.position;
			pos.y += deltaMagnitudeDiff * ZoomSpeed;
			pos.z += deltaMagnitudeDiff * ZoomSpeed;

			transform.position = pos;
			/*
			// If the camera is orthographic...
			if (cam.orthographic)
			{
				// ... change the orthographic size based on the change in distance between the touches.
				cam.orthographicSize += deltaMagnitudeDiff * orthoZoomSpeed;

				// Make sure the orthographic size never drops below zero.
				cam.orthographicSize = Mathf.Max(cam.orthographicSize, 0.1f);
			}
			else
			{
				// Otherwise change the field of view based on the change in distance between the touches.
				cam.fieldOfView += deltaMagnitudeDiff * perspectiveZoomSpeed;

				// Clamp the field of view to make sure it's between 0 and 180.
				cam.fieldOfView = Mathf.Clamp(cam.fieldOfView, 0.1f, 179.9f);
			}*/


		}

	}
}