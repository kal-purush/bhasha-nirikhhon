using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;

public class CameraFollow : MonoBehaviour {
	public Transform Target;
	public int FollowFrames = 20;
	public float SmoothTime = 0.5f;
	
	private Vector3[] positions;
	private Vector3 velocity = Vector3.zero;
	
	private void Awake() {
		positions = new Vector3[FollowFrames];
		
		for (int i = 0; i < positions.Length - 1; i++) {
			positions[i] = positions[i + 1];
		}
	}
	
	private void Update() {
		positions[positions.Length - 1] = new Vector3(Mathf.Round(Target.position.x * 10f) / 10f, 0, Mathf.Round(Target.position.z * 10f) / 10f);
		
		Vector3 direction = positions[positions.Length - 1] - positions[0];
		
		Vector3 target = positions[positions.Length - 1] - (3f * direction.normalized) + (3f * Vector3.up);
		
		transform.position = Vector3.SmoothDamp(transform.position, target, ref velocity, SmoothTime);
		transform.LookAt(Target.position);
	}
}