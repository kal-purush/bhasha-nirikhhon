using UnityEngine;
using System.Collections;

public class Brick : MonoBehaviour {

public int Maximum_Hits;
public int Times_Hit;
	// Use this for initialization
	void Start () {
	
	Times_Hit = 0;
	}
	
	void OnCollisionEnter2D(Collision2D collision)
		
	{
	Times_Hit = Times_Hit +1;
		print(" collision invoked ");
		
		
	} 
}