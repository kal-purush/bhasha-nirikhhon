using UnityEngine;
using System.Collections;

public class Move : MonoBehaviour {
    private Rigidbody2D c_body;
    private Animator c_anim;
    [SerializeField] private float speed = 100f;
    private bool facingRight = true;
    // Use this for initialization
    void Start() {
        c_body = GetComponent<Rigidbody2D>();
        c_anim = GetComponent<Animator>();
    }
    // Update is called once per frame
    void Update() {
        int direction = 0; //0 = not moving, 1 = up, 2 = down, 3 = left, 4 = right
        if(Input.GetKey(KeyCode.UpArrow) == true){ direction = 1;}
        if (Input.GetKey(KeyCode.DownArrow) == true) { direction = 2; }
        if (Input.GetKey(KeyCode.LeftArrow) == true) { direction = 3; }
        if (Input.GetKey(KeyCode.RightArrow) == true) { direction = 4; }

        switch (direction)
        {
            case 1: c_body.AddForce(new Vector2(0f, speed));
                break;
            case 2:
                c_body.AddForce(new Vector2(0f, -speed));
                break;
            case 3:
                c_body.AddForce(new Vector2(-speed, 0f));
                break;
            case 4:
                c_body.AddForce(new Vector2(speed, 0f));
                break;
        }
        if (direction != 0)
        {
            c_anim.SetBool("moving", true);
        }
        else c_anim.SetBool("moving", false);
        if(c_body.velocity.x > 0 && !facingRight)
        {
            facingRight = !facingRight;
            Vector3 theScale = transform.localScale;
            theScale.x *= -1;
            transform.localScale = theScale;
        }
        else if(c_body.velocity.x < 0 && facingRight)
        {
            facingRight = !facingRight;
            Vector3 theScale = transform.localScale;
            theScale.x *= -1;
            transform.localScale = theScale;
        }
    }
}