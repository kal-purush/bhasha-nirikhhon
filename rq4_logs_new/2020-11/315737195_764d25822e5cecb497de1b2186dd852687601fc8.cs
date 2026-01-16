using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraLerpToTransform : MonoBehaviour
{
	// 1 the target the camera will track, tracking, speed and the camera’s bounds.
	public Transform camTarget;
	public float trackingSpeed;
	public float minX;
	public float minY;
	public float maxX;
	public float maxY;
	// 2 FixedUpdate() is called with every fixed-framerate frame
	//  the camera will track the player, which will have a RigidBody2D attached to it.
	void FixedUpdate()
	{
		// 3 This null check ensures that a valid Transform component was assigned to the camTarget field on the script in the editor.
		if (camTarget != null) 
		{
			// 4 Vector2.Lerp() performs linear interpolation between two vectors by the third parameter’s value (a value between 0 and 1).
			// The rest of the Lerp method ensures that the position calculated is clamped (constrained) to the MinX, MinY, MaxX, and MaxY points.
			var newPos = Vector2.Lerp(transform.position,
			camTarget.position,
			Time.deltaTime * trackingSpeed);
			var camPosition = new Vector3(newPos.x, newPos.y, -10f);
			var v3 = camPosition;
			var clampX = Mathf.Clamp(v3.x, minX, maxX);
			var clampY = Mathf.Clamp(v3.y, minY, maxY);
			transform.position = new Vector3(clampX, clampY, -10f);
		}
	}
}