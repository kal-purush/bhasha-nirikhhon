using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraController : MonoBehaviour {

	public int zoomLevel = 1;
	public int maxZoomLevel = 12;
	public int minZoomLevel = 1;

	private bool isDragging = false;
	private Vector3 dragStartPosition;
	private Vector3 dragMousePosition;

	private Vector3 camDest;

	void Start() {

		camDest = transform.position;

	}

	private void Update() {

		HandlePanning();
		HandleScrolling();

		transform.position += ( camDest - transform.position ) / 15f;
		Camera.main.orthographicSize += ( zoomLevel - Camera.main.orthographicSize ) / 20f;

	}

	public void HandlePanning() {

		if( Input.GetMouseButton( 2 ) ) {

			if( !isDragging ) {

				isDragging = true;
				dragStartPosition = transform.position;
				dragMousePosition = Camera.main.ScreenToWorldPoint( Input.mousePosition );

			}

			Vector3 panOffset = dragMousePosition - Camera.main.ScreenToWorldPoint( Input.mousePosition );

			camDest += panOffset;
			dragMousePosition = Camera.main.ScreenToWorldPoint( Input.mousePosition );

		} else {

			isDragging = false;

		}

	}

	private void HandleScrolling() {

		float d = Input.GetAxis( "Mouse ScrollWheel" );

		if( d < 0 ) {
			zoomLevel++;
			if( zoomLevel > maxZoomLevel ) {
				zoomLevel = maxZoomLevel;
			}
		} else if( d > 0 ) {
			zoomLevel--;
			if( zoomLevel < minZoomLevel ) {
				zoomLevel = minZoomLevel;
			}

		}

	}
}