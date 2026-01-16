using System.Collections;
using System.Collections.Generic;
using UnityEngine;


public class HealthPickup : MonoBehaviour
{
    public void Die()
    {
        Destroy(gameObject);
    }
}