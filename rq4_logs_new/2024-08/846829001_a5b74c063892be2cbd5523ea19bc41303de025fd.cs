
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.TextCore.Text;

public class PlayerMovement : MonoBehaviour
{
    public GameObject character;
    public Rigidbody2D rb;
    public float speed;
    public float jump_force = 8f;
    public float radius = 3f;


    [SerializeField] private Transform groundcheck;

    [SerializeField] private LayerMask groundlayer;



    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }


    void Update()
    {
        print(IsGrounded());
        if (Input.GetKey(KeyCode.RightArrow))
        {
            character.transform.Translate(new Vector3(speed * Time.deltaTime, 0, 0));

        }
        else if (Input.GetKey(KeyCode.LeftArrow))
        {
            character.transform.Translate(new Vector3(-speed * Time.deltaTime, 0, 0));


        }
        if (Input.GetKeyDown(KeyCode.W) && IsGrounded())
        {
            rb.velocity = new Vector2(rb.velocity.x, jump_force);

        }
    }




    //&& IsGrounded())
    private bool IsGrounded()
    {
        return Physics2D.OverlapCircle(groundcheck.position, radius, groundlayer);
    }
}