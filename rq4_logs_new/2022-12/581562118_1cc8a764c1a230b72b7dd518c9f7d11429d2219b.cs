using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Controls : MonoBehaviour
{
    void Update()
    {
        if (Input.GetMouseButtonDown(0))
        {
            Ray ray = Camera.main.ScreenPointToRay(Input.mousePosition);

            RaycastHit hit;
            if (Physics.Raycast(ray, out hit, Mathf.Infinity))
            {
                print(hit.collider.tag);
                if (hit.collider.tag == "card")
                {
                    Debug.Log(hit.collider.name);
                }
            }
        }
        else if (Input.GetMouseButtonUp(0))
        {

        }
    }
}
