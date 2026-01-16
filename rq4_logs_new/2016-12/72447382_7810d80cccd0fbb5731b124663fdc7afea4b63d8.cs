using UnityEngine;
using System.Collections;

public class PointLightBehaviour : MonoBehaviour {

    private Light pointLight;
    private float originalIntensity;
    public float timeToDark = 2f;

	// Use this for initialization
	void Start () {
        pointLight = transform.GetComponent<Light>();
        originalIntensity = pointLight.intensity;
    }
	
	// Update is called once per frame
	void Update () {
        pointLight.intensity = pointLight.intensity - originalIntensity * Time.deltaTime * 1 / timeToDark;
    }
}