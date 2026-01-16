using UnityEngine;
using System.Collections;

public class Scan : MonoBehaviour {

    public Vector2 startingPos;
    public int scanSize = 50;
    public float scanSpeed = 0.25f;
    private float minBound;
    private float maxBound;
    private bool movingForward = true;

    // Use this for initialization
    void Start () {
        startingPos = transform.position;
        minBound = startingPos.x - scanSize;
        maxBound = startingPos.x + scanSize;
    }

    void Update ()
    {
        if (transform.position.x < minBound && !movingForward)
        {
            movingForward = true;
        }
        else if (transform.position.x > maxBound && movingForward)
        {
            movingForward = false;
        }
    }
    
    void FixedUpdate () {
        if (movingForward)
        {
            transform.position = new Vector3(transform.position.x + scanSpeed, transform.position.y, transform.position.z);
        }

        else
        {
            transform.position = new Vector3(transform.position.x - scanSpeed, transform.position.y, transform.position.z);

        }
    }
}