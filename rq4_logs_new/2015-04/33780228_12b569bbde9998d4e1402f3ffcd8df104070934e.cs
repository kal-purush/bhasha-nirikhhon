using UnityEngine;
using System.Collections;

public class CameraShake : MonoBehaviour {

	private Vector3 defaultPosition;
	private bool shaking;
	private float intensity;
	private float reduction;
	private Vector3 direction;

	// Use this for initialization
	void Start () {
		defaultPosition = transform.localPosition ;
		Reset();
	}
	
	// Update is called once per frame
	void Update () {
		if (shaking) {
			if(intensity < 0.1f){
				Reset();
				transform.localPosition = defaultPosition;
			} else {
				if(direction == Vector3.zero){
					transform.localPosition = defaultPosition + new Vector3(Random.value*intensity-intensity/2, Random.value*intensity-intensity/2, 0f);
				} else {
					transform.localPosition = defaultPosition + new Vector3(Random.value*intensity*direction.x, Random.value*intensity*direction.y, 0f);
				}
				intensity = (0-intensity)*(reduction*Time.deltaTime)+intensity;
			}
		}
	}

	void Reset(){
		shaking = false;
		intensity = 0f;;
		reduction = 0f;;
		direction = new Vector3();
	}

	public void ShakeCamera(float intensity, float reduction, Vector3 direction){
		this.intensity = intensity;
		this.reduction = reduction;
		this.direction = direction;
		shaking = true;
	}
}