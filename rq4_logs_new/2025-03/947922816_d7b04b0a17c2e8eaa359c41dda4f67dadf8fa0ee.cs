using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class mousechase : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        RaycastHit hit;
        if(Physics.Raycast(Camera.main.ScreenPointToRay(Input.mousePosition), out hit, 100))
        {
            transform.position = hit.point;
        }
    }
}