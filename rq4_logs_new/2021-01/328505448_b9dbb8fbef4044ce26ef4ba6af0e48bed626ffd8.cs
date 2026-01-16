using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraFollow : MonoBehaviour
{
    // Start is called before the first frame update

    public Transform playerPos;
    public GameObject ball;

    // Update is called once per frame
    void Update()
    {
        
        if(ball!=null)
        {
            transform.position = new Vector3(transform.position.x, playerPos.position.y, transform.position.z);
        }
    }
}