using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI; 

public class TutorialCube : MonoBehaviour
{
    [Header("Physics Settings")]
    private Color startingColor;
    private Color responseColor;

    // Start is called before the first frame update
    void Start()
    {
        startingColor = GetComponent<Renderer>().material.color;
    }

    // Update is called once per frame
    void Update()
    {
        //
    }

    void DestroyGameObject()
    {
        Destroy(gameObject);
    }
    
    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "TutorialDoor")
        {
            if (other.GetComponent<Renderer>().material.color == Color.green)
            {
                responseColor = Color.green;
                Destroy(other.gameObject);
                Destroy(gameObject);
                GetComponent<Renderer>().material.color = startingColor;
                Debug.Log("Tutorial finished!");
            }
            else
            {
                responseColor = Color.red;
                Debug.Log("User attempted to bypass tutorial!");
            }
        }
        else if (other.gameObject.tag == "TutorialPlatform")
        {
            
            if (other.GetComponent<Renderer>().material.color == Color.green)
            {
                //
            }
            else
            {
                other.enabled = false;
            }
        }
        else
        {
            return;
        }
    }
}