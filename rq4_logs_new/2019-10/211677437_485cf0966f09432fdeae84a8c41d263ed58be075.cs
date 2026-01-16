using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RandomLocations : MonoBehaviour
{
    //Map Boundaries
    GameObject upperBoundaryCollider;
    GameObject lowerBoundaryCollider;
    GameObject leftBoundaryCollider;
    GameObject rightBoundaryCollider;

    float upperBoundary;
    float lowerBoundary;
    float leftBoundary;
    float rightBoundary;

    // Start is called before the first frame update
    void Start()
    {
        upperBoundaryCollider = GameObject.Find("WallTop");
        lowerBoundaryCollider = GameObject.Find("WallDown");
        leftBoundaryCollider = GameObject.Find("WallLeft");
        rightBoundaryCollider = GameObject.Find("WallRight");

        upperBoundary = upperBoundaryCollider.transform.position.y;
        //Debug.Log("upperBoundary: " + upperBoundary);

        lowerBoundary = lowerBoundaryCollider.transform.position.y;
        //Debug.Log("lowerBoundary: " + lowerBoundary);

        rightBoundary = rightBoundaryCollider.transform.position.x;
        //Debug.Log("rightBoundary: " + rightBoundary);

        leftBoundary = leftBoundaryCollider.transform.position.x;
        //Debug.Log("leftBoundary: " + leftBoundary);

        float randomPositionX = Random.Range(leftBoundary, rightBoundary);
        float randomPositionY = Random.Range(lowerBoundary, upperBoundary);

        Vector3 temp = new Vector3(randomPositionX, randomPositionY, 0);
        transform.position = temp;
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}