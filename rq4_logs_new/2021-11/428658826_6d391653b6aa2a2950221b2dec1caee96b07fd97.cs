using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WaterWall : MonoBehaviour
{
    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.TryGetComponent<Torch>(out Torch t))
        {
            if (other == t.sphere && t.On)
            {
                t.Switch();
            }
        }
    }
}