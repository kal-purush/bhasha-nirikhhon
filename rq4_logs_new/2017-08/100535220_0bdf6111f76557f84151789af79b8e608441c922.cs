using System.Collections;
using System.Collections.Generic;
using UnityEngine;

//Info for a single farm block.
public class FarmBlockInfo : MonoBehaviour {

    //States of the block
    public bool PLANTED = false, WATERED = false, TILLED = false;

    //Coordinate of the block in this plot
    public Vector2 coordinate;

	// Use this for initialization
	void Start () {
        //Parse coordinate from the name.
        string name = transform.name;

        int x = int.Parse(name.Substring(1, 1));
        int y = int.Parse(name.Substring(3, 1));

        coordinate = new Vector2(x, y);
	}

    private void OnCollisionEnter(Collision collision)
    {
        
    }
}