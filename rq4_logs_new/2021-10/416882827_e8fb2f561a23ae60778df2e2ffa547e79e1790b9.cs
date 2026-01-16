using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BallDissabled : MonoBehaviour
{
    public GameObject ball;

    void OnTriggerEnter(Collider other)
    {
        if(other.gameObject.tag == "Player")
        {
            ball.SetActive(false);
        }
    }

    void OnTriggerExit(Collider other)
    {
        if(other.gameObject.tag == "Player")
        {
            ball.SetActive(true);
        }
    }
}