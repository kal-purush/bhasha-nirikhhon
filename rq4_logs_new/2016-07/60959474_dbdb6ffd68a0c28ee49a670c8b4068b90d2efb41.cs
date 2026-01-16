using UnityEngine;
using System.Collections;

public class RestPointScript : MonoBehaviour {

    private bool occupied; // is a friendly using this rest point
    public bool Occupied
    {
        get { return occupied; }
    }

    public void OnTriggerEnter(Collider other)
    {
        occupied = true;
    }
    public void OnTriggerExit(Collider other)
    {
        occupied = false;
    }
}