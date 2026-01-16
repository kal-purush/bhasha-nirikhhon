using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Scr_AstronautResourcesCheck : MonoBehaviour
{
    public List<GameObject> resourceList = new List<GameObject>();

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if(collision.gameObject.tag == "Resources")
        {
            resourceList.Add(collision.gameObject);
        }
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if(collision.gameObject.tag == "Resources")
        {
            for(int i = 0; i < resourceList.Count; i++)
            {
                if(resourceList[i].name == collision.gameObject.name)
                    resourceList.RemoveAt(i);
            }
        }
    }
}