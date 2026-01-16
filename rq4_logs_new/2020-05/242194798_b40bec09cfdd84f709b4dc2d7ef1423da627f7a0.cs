using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WorldSwapSaver : MonoBehaviour
{
	[SerializeField]
	private GameObject labTrigger;
	[SerializeField]
	private GameObject worldTrigger;

	[SerializeField]
	private GameObject lab;
	[SerializeField]
	private GameObject world;

	[SerializeField]
	private bool isInLab;
	[SerializeField]
	private bool isInWorld;

	private void Start()
	{
		
	}

	private void OnTriggerEnter(Collider other)
	{
		if (isInLab == true && isInWorld == false)
		{
			lab.SetActive(true);
			world.SetActive(false);
		}
		else if (isInLab == false && isInWorld == true)
		{
			lab.SetActive(false);
			world.SetActive(true);
		}

	}
}