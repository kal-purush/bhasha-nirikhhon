using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CreacionProcedural : MonoBehaviour {

    public GameObject[] tiles;
    public int[] orden;

    private Vector3 puntoInicio;

	// Use this for initialization
	void Start () {
		for(int i=0; i <orden.Length; i++)
        {
            GameObject tile = Instantiate(tiles[orden[i]], puntoInicio, Quaternion.identity);
            puntoInicio = tile.transform.Find("Helper").position;
        }
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}