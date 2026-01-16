using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LightBeam : MonoBehaviour 
{
	private LineRenderer lineRenderer;
	private Transform trans;
	[SerializeField] float maxDistance = 5000.0f;
	PlayerController playerScript;

	void Start()
	{
		trans = GetComponent<Transform>();
		lineRenderer = GetComponent<LineRenderer>();

		playerScript = GameManager.instance.Player.GetComponent<PlayerController>();
	}

	void Update()
	{
		lineRenderer.SetPosition(0, trans.position);

		RaycastHit hit;
		if (Physics.Raycast(trans.position, trans.right, out hit))
		{
			if (hit.collider)
			{
				lineRenderer.SetPosition(1, hit.point);
			}
			if (hit.collider.tag == "Player")
			{
				playerScript.KillPlayer();
			}
		}
		else
		{
			lineRenderer.SetPosition(1, transform.right * maxDistance);
		}

	}
}