using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NextQuestion : MonoBehaviour
{
    private Color response;

    void DestroyGameObject()
    {
        Destroy(gameObject);
    }

    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "AnswerCube")
        {
            if (other.GetComponent<Renderer>().material.color == Color.green)
            {
                response = Color.green;
                Destroy(gameObject);
                Destroy(other);
                Debug.Log("Next question unlocked!");
            }
            else
            {
                response = Color.red;
                Debug.Log("User attempted to unlock next question");
            }
        }
        else
        {
            return;
        }
    }
}