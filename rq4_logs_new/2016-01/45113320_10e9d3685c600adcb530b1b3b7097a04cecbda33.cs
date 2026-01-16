using UnityEngine;
using System.Collections;

public class Loot : MonoBehaviour {

    public float speed;
    public float jumpForce;
    public float gravity;
    public bool ground = false;
    private Vector3 moveDirection = Vector3.zero;
    private float gravityVelocity = 0f;

    private float minY = 0f;
    private float minX = 0f;
    private float maxX = 0f;

    void Start()
    {
        Camera cam = Camera.main;
        minY = cam.ScreenToWorldPoint(new Vector3(0f, 0f, 0f)).y;
        minX = cam.ScreenToWorldPoint(new Vector3(0f, 0f, 0f)).x;
        minX += transform.GetChild(0).localScale.x * 2f;
        maxX = cam.ScreenToWorldPoint(new Vector3(Screen.width, 0f, 0f)).x;
        maxX -= transform.GetChild(0).localScale.x * 2f;
    }


    void Update()
    {
        if (transform.position.y == minY)
        {
            gravityVelocity = 0f;
        }
        else
        {
            gravityVelocity -= gravity * Time.deltaTime;
        }
        moveDirection.y += gravityVelocity;
        moveDirection = transform.TransformDirection(moveDirection);
        moveDirection *= speed;

        float X = Mathf.Clamp(transform.position.x + (moveDirection * Time.deltaTime).x, minX, maxX);
        float Y = Mathf.Max(transform.position.y + (moveDirection.y * Time.deltaTime), minY);
        transform.position = new Vector3(X, Y, 0f);
    }
}