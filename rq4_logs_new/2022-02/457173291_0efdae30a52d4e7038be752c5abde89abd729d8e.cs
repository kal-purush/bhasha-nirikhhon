using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class StickyPlatform : MonoBehaviour
{
    public static bool isNotOnMovingPlatform = true;
    // Start is called before the first frame update
    private void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.name == "MaleFreeSimpleMovement1")
        {
            isNotOnMovingPlatform = false;
            collision.gameObject.transform.SetParent(transform);
            
        }
    }

    // Update is called once per frame
    private void OnCollisionExit(Collision collision)
    {
        if (collision.gameObject.name == "MaleFreeSimpleMovement1")
        {
            isNotOnMovingPlatform = true;
            collision.gameObject.transform.SetParent(null);
        }
    }
}