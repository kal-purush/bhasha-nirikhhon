using UnityEngine;
using System.Collections;

public class Test_Pickup : MonoBehaviour
{
    void OnTriggerEnter(Collider other)
    {
        IPickUp pickUp = other.gameObject.GetComponent<IPickUp>();
        if (pickUp != null)
        {
            pickUp.OnPull(gameObject);
        }
    }
}