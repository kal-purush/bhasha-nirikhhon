using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MapElement : MonoBehaviour {

    public Vector2 mapCords;
    public 
	// Use this for initialization
	void Start () {
     
	}
	
	// Update is called once per frame
	void Update () {
		
	}
    private void OnMouseEnter()
    {
        transform.position = new Vector3(transform.position.x, 0.05f, transform.position.z); 
    }
   
    private void OnMouseExit()
    {
        transform.position = new Vector3(transform.position.x, 0.0f, transform.position.z);
    }

    private void OnMouseDown()
    {
        Debug.Log("You clicked the map element of with coordinates: "+mapCords.x+", "+mapCords.y);
    }
}