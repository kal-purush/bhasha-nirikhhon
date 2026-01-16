using UnityEngine;
using System.Collections;

public class LightController : MonoBehaviour
{

    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }

    void OnMouseOver()
    {
        if (Input.GetMouseButtonDown(0))
        {
            if (GetComponentInParent<TrafficLight>().straightGreen == true)
                GetComponentInParent<TrafficLight>().straightGreen = false;
            else
                GetComponentInParent<TrafficLight>().straightGreen = true;
        }
    }
}