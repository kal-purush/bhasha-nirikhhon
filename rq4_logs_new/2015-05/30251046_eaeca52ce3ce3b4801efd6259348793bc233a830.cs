using UnityEngine;
using System.Collections;

public class DestroyHole : MonoBehaviour {
	
	void Start () 
	{
	
	}

	void Update () 
	{
		Destroy (gameObject, 120.0f);
	}
}