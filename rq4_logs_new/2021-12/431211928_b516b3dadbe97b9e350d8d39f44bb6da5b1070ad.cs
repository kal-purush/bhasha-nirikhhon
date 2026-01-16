using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PressurePlate : MonoBehaviour
{
    public GameObject slidedoor;
    public Vector3 position;
    public Quaternion rotation;

    public bool active;
    // Start is called before the first frame update
    private void OnCollisionEnter(Collision collision)
    {
        if (collision.transform.tag =="Prop" && !active)
        {
            slidedoor.transform.position -= position;

            active = true;
        }
    }

    private void OnCollisionExit(Collision collision)
    {
        if (collision.transform.tag == "Prop" && active)
        {
            slidedoor.transform.position += position;
            active = false;
        }
    }

}