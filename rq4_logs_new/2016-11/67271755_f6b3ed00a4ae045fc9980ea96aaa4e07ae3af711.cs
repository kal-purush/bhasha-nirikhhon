using UnityEngine;
using System.Collections;

// Written and modified by Fadi Yousif
// attached to the windows in the main room

public class Enlarge : MonoBehaviour {

    private Vector3 Original_Scale; // holds the original scale at the start of the game
    public float Enlarge_Size;      // value to enlarge the object by

    // Use this for initialization
    void Start () {
        Original_Scale = transform.localScale; // gets the original scale
    }

    public void Inlarge()
    {
        transform.localScale = Original_Scale * Enlarge_Size;
    }

    public void Dislarge()
    {
        transform.localScale = Original_Scale;
    }
}