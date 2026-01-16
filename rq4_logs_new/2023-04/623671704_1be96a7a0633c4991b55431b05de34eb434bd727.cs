using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RoboScript : MonoBehaviour
{
    public Rigidbody2D myRigidbody;
    public Vector2 min_momentum = new Vector2(5, 5);
    public float move_strength = 40;
    public float max_speed = 12;
    public float decay_rate = 25;
    public float time_scale = 10;
    // Start is called before the first frame update
    void Start()
    {
        gameObject.name = "His Robotness";
    }

    // Update is called once per frame
    void Update()
    {
        // Used to normalize updates across various frame-rates
        float time_step = time_scale * Time.deltaTime;

        // Constructs a unit-vector along one of the eight digital directions
        Vector2 acceleration = Vector2.up * Input.GetAxisRaw("Vertical") + Vector2.right * Input.GetAxisRaw("Horizontal");
        acceleration = acceleration.normalized;

        // Updates speed based on move_strength, independant of decay_rate
        acceleration *= (move_strength + decay_rate) * time_step;
        
        // Calculates decay vector and adjusts acceleration accordingly
        acceleration += calc_decay_vector(time_step);

        // Scale back the speed of the adjusted velocity if needed
        myRigidbody.velocity = Vector2.ClampMagnitude(
            myRigidbody.velocity + acceleration,
            max_speed
        );
    }
    Vector2 calc_decay_vector(float time_step){
        Vector2 my_vel = myRigidbody.velocity;
        float decay_val = decay_rate * time_step;
        float final_speed = Mathf.Max(my_vel.magnitude - decay_val, 0);
        return Vector2.ClampMagnitude(my_vel, final_speed) - my_vel;
    }
}