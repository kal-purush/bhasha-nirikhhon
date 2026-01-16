using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Money_Change : MonoBehaviour
{
    int timer = 15;

	// Use this for initialization
	void Start ()
    {
        
	}
	
	// Update is called once per frame
	void Update ()
    {
        timer--;

        if(timer % 2 == 0)
        {
            transform.position = new Vector3(transform.position.x, transform.position.y + .1f, transform.position.z);
            GetComponent<Text>().color = new Color(GetComponent<Text>().color.r, GetComponent<Text>().color.g, GetComponent<Text>().color.b, GetComponent<Text>().color.a - .05f);
        }

        if(timer <= 0)
        {
            Destroy(gameObject);
        }
	}
}