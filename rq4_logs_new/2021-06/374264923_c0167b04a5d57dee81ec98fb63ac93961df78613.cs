using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FuelCanIncrementor : MonoBehaviour
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
            Debug.Log("Before Increment: " + fuelCapacity.fuel.ToString());
            fuelCapacity.FuelCollection(20f);
            // fuelCapacity.FuelCollection(10f);
            Debug.Log("After Increment: " + fuelCapacity.fuel.ToString());
            Destroy(this.gameObject);
        }
    }
}