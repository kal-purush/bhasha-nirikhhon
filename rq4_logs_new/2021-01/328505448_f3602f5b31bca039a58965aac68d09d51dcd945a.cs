using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Portal : MonoBehaviour
{
    private Transform destination;
    public bool isSecond;
    // Start is called before the first frame update
    void Start()
    {
        if(isSecond==false)
        {
            destination = GameObject.FindGameObjectWithTag("secondPortal").GetComponent<Transform>();
        }
        else
        {
            destination = GameObject.FindGameObjectWithTag("firstPortal").GetComponent<Transform>();

        }
    }

    void OnTriggerEnter2D(Collider2D other)
    {
        if (Vector2.Distance(transform.position,other.transform.position)>0.3f)
        {
            other.transform.position = new Vector2(destination.position.x, destination.position.y);
        }
    }
    // Update is called once per frame
    void Update()
    {
        
    }
}