using System.Collections;
using UnityEngine;

public class ApplicationHandler : MonoBehaviour
{
	public GameObject Controller;

	void Start()
	{
		StartCoroutine(ActivateController());
	}

	private IEnumerator ActivateController()
	{
		yield return new WaitForSeconds(2);
		Controller.gameObject.SetActive(true);
	}
}