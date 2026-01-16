using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Blocking : MonoBehaviour
{
    public LayerMask projectileLayer;

    private void OnTriggerEnter(Collider other)
    {
        if(other.gameObject.layer == projectileLayer)
        {
            Debug.Log(other.name + " is at " + other.bounds.center);
        }
        //RaycastHit hit;
        //if (Physics.Raycast(transform.position, transform.forward, out hit, 100.0f, basicEnemyLayer))
        //{
        //    Debug.Log(hit.collider.gameObject.name + " Point of contact: " + hit.point);
        //}
    }
}