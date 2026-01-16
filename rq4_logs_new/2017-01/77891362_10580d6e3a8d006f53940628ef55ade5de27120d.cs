using UnityEngine;
using System.Collections;

public class CameraManager : MonoBehaviour {


	public float newZoom;
	public Vector3 newTarget;
	public float newAngle;
	public float defaultZoom;

	public float moveSpeed;
	public float rotateSpeed;
	private float currentAngle;

	public Transform up;
	public Transform camera;

	void Start(){
		currentAngle = 0;
		defaultZoom = 10;
		newZoom = defaultZoom * -1;
		camera.localPosition = Vector3.forward * newZoom;
	}

	public void SlideToPosition(Vector3 target){
		newTarget = target;
	}

	public void JumpToPosition(Vector3 target){
		newTarget = target;
		transform.position = target;
	}

	public void SlideToRotation(float angle){
		newAngle = angle;
	}


	// Update is called once per frame
	void Update () {
		transform.position = Vector3.Lerp (transform.position, newTarget, moveSpeed * Time.deltaTime);
		currentAngle = Mathf.Lerp (currentAngle, newAngle, rotateSpeed * Time.deltaTime);
		transform.localEulerAngles = new Vector3 (0,currentAngle,0);
		newZoom += Input.GetAxis ("Mouse ScrollWheel") * 10;
		camera.localPosition = new Vector3 (0,0,Mathf.Lerp(camera.localPosition.z, newZoom, 4 * Time.deltaTime));
	}
}