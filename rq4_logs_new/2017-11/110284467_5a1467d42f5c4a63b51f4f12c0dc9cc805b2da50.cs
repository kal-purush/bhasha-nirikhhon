using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Mirror : MonoBehaviour 
{
	Transform trans;
	[SerializeField] private float turnSpeed;

	private bool playerIsTouching = false;

	void Start()
	{
		trans = GetComponent<Transform>();
	}

	void TurnMirror(int direction)
	{
		if (direction == 0)
		{
			trans.Rotate(Vector3.up, -turnSpeed * Time.deltaTime);
		}
		else if (direction == 1)
		{
			trans.Rotate(Vector3.up, turnSpeed * Time.deltaTime);
		}
		
	}

	void Update()
	{
		if (playerIsTouching)
		{
			if (Input.GetKey(KeyCode.Q))
			{
				TurnMirror(0);
			}
			else if (Input.GetKey(KeyCode.E))
			{
				TurnMirror(1);
			}
		}
	}

	void OnTriggerEnter(Collider col)
	{
		if (col.gameObject.tag == "Player")
		{
			playerIsTouching = true;
		}
	}
	void OnTriggerExit(Collider col)
	{
		if (col.gameObject.tag == "Player")
		{
			playerIsTouching = false;
		}
	}
}