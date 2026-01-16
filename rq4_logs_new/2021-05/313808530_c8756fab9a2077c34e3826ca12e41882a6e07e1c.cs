using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GirlMovement2 : MonoBehaviour
{
    private Rigidbody2D rb;
    private float moveH;
    private Vector2 direction;
    [SerializeField] private float moveSpeed = 1.0f;
    // Start is called before the first frame update
    void Awake(){
        rb = GetComponent<Rigidbody2D>();
    }

    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (GameManager.instance.stopMoving)
        {
            rb.velocity = Vector2.zero;
        }
        moveH = Input.GetAxisRaw("Horizontal");
        direction = new Vector2(moveH, 0);
        rb.velocity = direction * moveSpeed;  

    }


}