using UnityEngine;
using System.Collections;

public class CameraFollow : MonoBehaviour {
	public float y;
	public float z;
	public float x;
	public float cos;

	public float howHighLookAt;
	public GameObject CameraRotateAroundBox;

	// Use this for initialization
	void Start () {

		x = 3 * Mathf.Cos(cos);
		z = 3 * Mathf.Sin(cos);
		CameraRotateAroundBox.gameObject.transform.position = new Vector3(transform.position.x + x, y, transform.position.z + z);
	}

	void GetRotationSideways(){
		x = 3 * Mathf.Cos(cos);
		z = 3 * Mathf.Sin(cos);
	}
	// Update is called once per frame
	void Update () {

		if (Input.GetKey(KeyCode.Q)){
			cos += 0.05f;
			GetRotationSideways();

		}
		else if (Input.GetKey(KeyCode.E)){
			cos -= 0.05f;
			GetRotationSideways();
		}
		CameraRotateAroundBox.gameObject.transform.position = new Vector3(transform.position.x + x, y, transform.position.z + z);
		CameraRotateAroundBox.gameObject.transform.LookAt(transform.position);
	}
}