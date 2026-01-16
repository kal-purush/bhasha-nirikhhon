using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CloudDamage : MonoBehaviour
{
    private FuelCapacity fuelCapacity;

    void Start()
    {
        fuelCapacity = GameObject.Find("Air_Balloon").GetComponent<FuelCapacity>();
    }
    private void OnTriggerEnter(Collider collider)
    {
        if (collider.gameObject.name == "Air_Balloon")
        {
            fuelCapacity.reductionRatio = 3.5f;
        }
    }
    private void OnTriggerExit(Collider collider)
    {
        if (collider.gameObject.name == "Air_Balloon")
        {
            fuelCapacity.reductionRatio = 2f;
        }
    }
}