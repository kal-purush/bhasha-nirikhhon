using UnityEngine;
using System.Collections;

public class LockRotation : MonoBehaviour {

	public GameObject target;
	public Vector3 buffer;
	public Quaternion rotate;

	void Start () {
		gameObject.SetActive (false);
		rotate = transform.rotation;
	}

	void Update() {
		transform.rotation = rotate;
		transform.position = transform.parent.transform.position + buffer;
	}

	public void SetTarget(GameObject g){
		target = g;
	}
}