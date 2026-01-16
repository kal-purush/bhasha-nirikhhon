using UnityEngine;
using System.Collections;

public class CameraBillboard : MonoBehaviour {
	
	// Update is called once per frame
	public Camera m_Camera;

	void Update()
	{
		m_Camera = Camera.main;
		transform.LookAt(transform.position + m_Camera.transform.rotation * Vector3.forward,
			m_Camera.transform.rotation * Vector3.up);
	}
}