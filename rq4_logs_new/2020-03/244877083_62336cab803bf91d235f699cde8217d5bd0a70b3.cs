using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PickupScript : MonoBehaviour
{
    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "Player")
        {
            if (other.gameObject.GetComponent<PlayerData>())
            {
                other.gameObject.GetComponent<PlayerData>().coins++;
                Debug.Log(other.gameObject.GetComponent<PlayerData>().coins);
            }
        }
        Destroy(this.gameObject);
    }
}