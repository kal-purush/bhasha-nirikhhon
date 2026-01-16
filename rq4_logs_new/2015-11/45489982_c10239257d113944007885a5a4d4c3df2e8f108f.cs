using UnityEngine;
using System.Collections;

public class CameraController : MonoBehaviour {
	
	public float dampTime = 0.15f;
	private Vector3 velocity = Vector3.zero;
	public Transform target;
	
	void FixedUpdate () 
	{
		if (target)
		{
			Vector3 point = GetComponent<Camera>().WorldToViewportPoint(target.position);
			Vector3 delta = target.position - GetComponent<Camera>().ViewportToWorldPoint(new Vector3(0.5f, 0.5f, point.z)); //(new Vector3(0.5, 0.5, point.z));
			Vector3 destination = transform.position + delta;
			//transform.position = Vector3.SmoothDamp(transform.position, destination, ref velocity, dampTime);
			Vector3 destPos = destination;
			destPos.x = Mathf.Clamp(destination.x, -12.4f, 12.4f);
			destPos.y = Mathf.Clamp(destination.y, -16.3f, 16.3f);
			transform.position = Vector3.SmoothDamp(transform.position, destPos, ref velocity, dampTime);
		}
	}
}