using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MirrorSpoon : MonoBehaviour
{
    public Transform spoon;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        Vector3 spoonLocalPosition = spoon.localPosition;
        float z = spoonLocalPosition.z;
        if (z < 0.25)
            z = 0.25f;
        transform.localPosition = new Vector3(
            (-1) * spoonLocalPosition.x,
            spoonLocalPosition.y + 0.03f,
            z
        );

        Vector3 currentEulerAngles = transform.eulerAngles;
        Vector3 spoonEulerAngles = spoon.eulerAngles;

        transform.eulerAngles = new Vector3(
            spoonEulerAngles.x,
            currentEulerAngles.y,
            currentEulerAngles.z
        );
    }
}