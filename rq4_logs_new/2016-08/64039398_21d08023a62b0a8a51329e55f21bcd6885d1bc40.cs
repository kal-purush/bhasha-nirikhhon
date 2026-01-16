using UnityEngine;
using System.Collections;

public class CameraShake : MonoBehaviour {

    private const float SHAKE_DURATION = 0.3f;
    private const float SHAKE_INTENSITY = .2f;
    private float shakeDuration;

	// Use this for initialization
	void Start () {
	    
	}
	
	// Update is called once per frame
	void Update () {
        shakeDuration -= Time.deltaTime;

        if(shakeDuration >= 0)
        {
            ShakeCamera();
        }else
        {
            transform.position = new Vector3(0,0,-10);
        }
	}

    public void SetShake()
    {
        shakeDuration = SHAKE_DURATION;
    }


    void ShakeCamera()
    {
        Vector2 shakeIntensity = new Vector2(Random.Range(-SHAKE_INTENSITY,SHAKE_INTENSITY),Random.Range(-SHAKE_INTENSITY, SHAKE_INTENSITY));
        transform.position = new Vector3(transform.position.x + shakeIntensity.x, transform.position.y + shakeIntensity.y, transform.position.z);

    }
}