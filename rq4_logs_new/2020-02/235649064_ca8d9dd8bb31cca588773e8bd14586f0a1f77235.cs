using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Shoot : MonoBehaviour
{
    public GameObject bullet;
    // Start is called before the first frame update
    void Start()
    {
    //    Instantiate(bullet, gameObject.transform);
        float waitSeconds = 0.025f;
        float firstCall = 1.0f;
        InvokeRepeating("ShootCannon", firstCall, waitSeconds);
    }

    void ShootCannon()
    {
        if(Input.GetMouseButtonDown(0)){
            GameObject newObj = Instantiate(bullet, gameObject.transform);
            Rigidbody rb = newObj.GetComponent<Rigidbody>();
            rb.velocity = new Vector3(5, 0, 0);
        }
    }
}