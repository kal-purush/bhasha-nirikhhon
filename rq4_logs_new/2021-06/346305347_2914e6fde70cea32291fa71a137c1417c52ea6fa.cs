using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HeadHelmet : MonoBehaviour
{
    public void Enable()
    {
        GetComponent<MeshRenderer>().enabled = true;
    }

    public void Disable()
    {
        GetComponent<MeshRenderer>().enabled = false;
    }
    // Update is called once per frame
    void Update()
    {
        float curAngle = transform.parent.rotation.eulerAngles.y;
        int angleBucket = Mathf.RoundToInt((8 * curAngle) / 360);
        transform.rotation = Quaternion.Euler(new Vector3(transform.rotation.eulerAngles.x, angleBucket * 45.0f, transform.rotation.eulerAngles.z));
    }
}