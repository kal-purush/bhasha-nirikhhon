using UnityEngine;
using System.Collections;

public class AccelerometerInput : MonoBehaviour {

    GameObject spaceship;
    float maxSpeed;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        
        // ROTATION // 

        float y = Input.acceleration.y; // y is mapped as pitch
        float z = Input.acceleration.z; // z is mapped as yaw
        // no need for roll in our implementation
        // wlog we can keep the spaceship upright at all times

        spaceship.transform.Rotate(new Vector3(0, y, z));

        // SPEED //

        if (Input.touchCount > 0 && Input.GetTouch(0).phase == TouchPhase.Moved)
        {
            // get movement of finger since last frame
            Vector2 touchDelta = Input.GetTouch(0).deltaPosition;
            float speed = 1 - touchDelta.y; // because of the orientation of our input devices vs Unity's default orientation, the y axis encodes
            // speed and we need to subtract it from 1
            // speed * maxSpeed now defines the speed of the spaceship

            // move spaceship along its forward axis
            spaceship.transform.Translate(Vector3.forward * speed * maxSpeed);
        }
	}
}