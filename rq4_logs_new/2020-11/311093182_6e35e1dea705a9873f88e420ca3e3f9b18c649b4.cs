using UnityEngine;
using System.Collections.Generic;

[ExecuteInEditMode]
public class SimpleIK : MonoBehaviour
{
	[SerializeField]
	private int iterations = 5;
	
	[SerializeField]
	[Range(0.01f, 1)]
	private float damping = 1;

	[SerializeField]
	private Transform target;
	[SerializeField]
	private Transform endTransform;

	void Start()
	{}

	void LateUpdate()
	{
		if (target == null || endTransform == null)
			return;

		int i = 0;

		while (i < iterations)
		{
			CalculateIK ();
			i++;
		}

		endTransform.rotation = target.rotation;
	}

	void CalculateIK()
	{		
		Transform node = endTransform.parent;

		while (true)
		{
			RotateTowardsTarget (node);

			if (node == transform)
				break;

			node = node.parent;
		}
	}

	void RotateTowardsTarget(Transform transform)
	{		
		Vector2 toTarget = target.position - transform.position;
		Vector2 toEnd = endTransform.position - transform.position;

		// Calculate how much we should rotate to get to the target
		float angle = SignedAngle(toEnd, toTarget);

		// Flip sign if character is turned around
		angle *= Mathf.Sign(transform.root.localScale.x);

		// "Slows" down the IK solving
		angle *= damping; 

		// Wanted angle for rotation
		angle = -(angle - transform.eulerAngles.z);

		transform.rotation = Quaternion.Euler(0, 0, angle);
	}

	public static float SignedAngle (Vector3 a, Vector3 b)
	{
		float angle = Vector3.Angle (a, b);
		float sign = Mathf.Sign (Vector3.Dot (Vector3.back, Vector3.Cross (a, b)));

		return angle * sign;
	}

	float ClampAngle (float angle, float min, float max)
	{
		angle = Mathf.Abs((angle % 360) + 360) % 360;
		return Mathf.Clamp(angle, min, max);
	}
}