using UnityEngine;
using System.Collections;

public class PlayerRotation : MonoBehaviour {

	public float rotateSpeed = 10f;
	Rigidbody2D _rigidbody;

	void Start()
	{
		_rigidbody = this.GetComponent<Rigidbody2D>();
	}

	void Update()
	{
		HandleRotation();
	}

	void HandleRotation()
	{
		//transform.rotation = Quaternion.Slerp (transform.rotation, transform.forward, rotateSpeed * Time.deltaTime);
		if(_rigidbody.velocity != Vector2.zero)
		{
			Vector3 lookVector = new Vector3(_rigidbody.velocity.x, _rigidbody.velocity.y, 0f).normalized;
			float lookAngle = Mathf.Rad2Deg * Mathf.Atan (lookVector.y/lookVector.x);
			if(lookVector.x < 0)
			{
				lookAngle += 180f;
			}
			//transform.rotation = Quaternion.Euler (0f,0f,lookAngle);
			Quaternion lookDirection = Quaternion.Euler (0f, 0f, lookAngle);
			transform.rotation = Quaternion.Slerp (transform.rotation, lookDirection, rotateSpeed * Time.deltaTime);
		}
	}
}