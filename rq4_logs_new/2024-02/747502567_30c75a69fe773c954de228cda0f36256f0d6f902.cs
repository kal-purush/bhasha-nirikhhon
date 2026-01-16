using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class parallax : MonoBehaviour
{
    public GameObject gameCamera;
    public float parallaxAmount;
    private float length, startPos, count;

    // Start is called before the first frame update
    void Start() {
        startPos = transform.position.x;
        length = GetComponent<SpriteRenderer>().bounds.size.x;
        count = 0;
    }

    // Update is called once per frame
    void FixedUpdate() {
        transform.position = new Vector3(startPos + gameCamera.transform.position.x * parallaxAmount, // add parallax effect
                                         transform.position.y + parallaxAmount * 0.01f * Mathf.Sin(count),
                                         //transform.position.y, // if virtical bobbing gets too much, use this instead
                                         transform.position.z);
        ;

        float loopCheck = gameCamera.transform.position.x * (1 - parallaxAmount); // check for looping
        if (loopCheck > startPos + length) { startPos += length; }
        else if (loopCheck < startPos - length) { startPos -= length; }
        count += 0.05f;
    }
}