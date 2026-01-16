using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Hand : MonoBehaviour
{
    const float MV_MAX_SPEED = 5f;
    const float MV_ACCEL = 100f;
    const float MV_FRICTION = 1f;
    const float RADIUS = 0.8f;

    GameObject grabbed_object;
    bool locked = false;

    // Update is called once per frame
    void Update()
    {
        if(Input.GetMouseButtonDown(0))
        {
            grab(); 
        }
    }

    private void FixedUpdate()
    {
        if(locked)
        {
            gameObject.GetComponent<Rigidbody2D>().velocity *= 0;
            return;
        }

        applyFriction();

        // Fetch user directional input
        Vector2 wish_dir = new Vector2(0, 0);
        if (Input.GetKey("d") || Input.GetKey("right"))
            wish_dir.x++;
        if (Input.GetKey("a") || Input.GetKey("left"))
            wish_dir.x--;
        if (Input.GetKey("w") || Input.GetKey("up"))
            wish_dir.y++;
        if (Input.GetKey("s") || Input.GetKey("down"))
            wish_dir.y--;
        wish_dir.Normalize();

        // Convert input to movement
        Vector2 acceleration = wish_dir;
        acceleration.x *= MV_ACCEL;
        acceleration.y *= MV_ACCEL;
        gameObject.GetComponent<Rigidbody2D>().velocity += acceleration;

        // TODO: Update the IK to match new position
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

    void grab()
    {
        RaycastHit hit;
        Ray ray = Camera.main.ScreenPointToRay(gameObject.GetComponent<RectTransform>().position);

        LayerMask lm = 0;
        //lm |= 1 << 9;
        lm = LayerMask.GetMask("grabbable");

        if (Physics.Raycast(ray, out hit, Mathf.Infinity, lm))
        {
            GameObject other = hit.transform.gameObject;

            //if(other.GetComponent<Head>())
            //{
            //    // You have just grabbed your own head
            //}
            if (other.GetComponent<Prop>() != null)
            {
                grabbed_object = other;
                other.GetComponent<Prop>().grab(gameObject.transform);
            }
            //if (other.GetComponent<PumpBar>() != null)
            //{
            //    locked = true;
            //    gameObject.transform.parent = other.transform;
            //    other.GetComponent<PumpBar>().grab(gameObject.transform);
            //}
        }
    }
}