using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Head : MonoBehaviour
{
    const float MV_ACCEL = 500f;
    const float MV_FRICTION = 1f;
    
    [SerializeField] int id = 0; // 0 = left, 1 = right

    private void FixedUpdate()
    {
        applyFriction();

        // Fetch user directional input
        Vector2 wish_dir = new Vector2(0, 0);
        if (Input.GetKey("right"))
            wish_dir.x++;
        if (Input.GetKey("left"))
            wish_dir.x--;
        if (Input.GetKey("up"))
            wish_dir.y++;
        if (Input.GetKey("down"))
            wish_dir.y--;
        wish_dir.Normalize();

        // Convert input to movement
        Vector2 acceleration = wish_dir;
        acceleration.x *= MV_ACCEL;
        acceleration.y *= MV_ACCEL;
        gameObject.GetComponent<Rigidbody2D>().velocity += acceleration;
    }
    
    private void applyFriction()
    {
        Vector2 vel = gameObject.GetComponent<Rigidbody2D>().velocity;
        float speed = vel.magnitude;
        if (speed < 0.01)
        {
            vel.x = 0;
            vel.y = 0;
            gameObject.GetComponent<Rigidbody2D>().velocity = vel;
            return;
        }

        float drop = speed * MV_FRICTION;

        float newspeed = speed - drop;
        if (newspeed < 0)
            newspeed = 0;
        newspeed /= speed;

        vel.x *= newspeed;
        vel.y *= newspeed;
        gameObject.GetComponent<Rigidbody2D>().velocity = vel;
    }
}