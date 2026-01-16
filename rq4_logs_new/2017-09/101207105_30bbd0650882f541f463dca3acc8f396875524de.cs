using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AddThisToPlayer : MonoBehaviour {

	public Transform attachObject;
	public GameObject assetArt;
	void OnTriggerEnter()
	{
		transform.parent = attachObject;
		transform.localPosition = Vector3.zero;
		transform.localRotation = Quaternion.identity;
		GetComponent<BoxCollider>().enabled = false;
	}

}