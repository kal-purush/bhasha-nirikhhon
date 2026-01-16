using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ZoneManager : MonoBehaviour
{
    public GameObject Area1Object;
    public BoxCollider Area1;
    public GameObject Area2Object;
    public BoxCollider Area2;
    public GameObject Area3Object;
    public BoxCollider Area3;

    private void Awake()
    {
        // Sets up the areas for the camera to access
        Area1 = Area1Object.GetComponent<BoxCollider>();
        Area2 = Area2Object.GetComponent<BoxCollider>();
        Area3 = Area3Object.GetComponent<BoxCollider>();
    }


    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}