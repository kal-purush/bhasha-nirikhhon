using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PlungerControl : MonoBehaviour
{
    // [SerializeField]
    // Slider powerSlider;
    [SerializeField]
    float maxPower;
    float minPower = 0f;
    float power;
    Vector3 force;
    List<Rigidbody> balls;
    
    // Start is called before the first frame update
    void Start()
    {
        // Initalise ball list
        balls = new List<Rigidbody>();
        power = 0f;
        // powerSlider.minValue = minPower;
        // powerSlider.maxValue = maxPower;
    }

    // Update is called once per frame
    void Update()
    {
        // If there are balls in the trigger
        if (balls.Count > 0) {
            // If space is pressed
            if (Input.GetKey(KeyCode.Space))
            {
                // Add power while it's less than the maximum
                // It will take 2 seconds to reach maximum
                power += maxPower * 0.5f * Time.deltaTime;
                if (power > maxPower) {
                    power = maxPower;
                }
            }
            // If space is released
            if (Input.GetKeyUp(KeyCode.Space))
            {
                Debug.Log("Launching ball!");
                // Add power while it's less than the maximum
                foreach (Rigidbody ball in balls)
                {
                    force = power * (-Vector3.forward);
                    Debug.Log("Force: " + force);
                    Debug.Log("Power: " + power);
                    Debug.DrawRay(ball.position, force * 10, new Color(255, 0, 0), 10f);
                    ball.AddForce(force);
                }
                // Reset power
                power = minPower;
            }
        }
    }

    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "ball")
        {
            Debug.Log("Ball is in plunger trigger");
            // Add ball to list of balls
            balls.Add(other.gameObject.GetComponent<Rigidbody>());
            Debug.Log("Balls: " + balls.Count);
        }
    }

    private void OnTriggerExit(Collider other)
    {
        if (other.gameObject.tag == "ball")
        {
            Debug.Log("Ball left plunger trigger");
            // Remove ball from list of balls
            balls.Remove(other.gameObject.GetComponent<Rigidbody>());
            Debug.Log("Balls: " + balls.Count);
        }
    }
}